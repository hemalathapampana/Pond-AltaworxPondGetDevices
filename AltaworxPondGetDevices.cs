using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Services.SQS;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models;
using Amop.Core.Models.Pond;
using Amop.Core.Repositories;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Pond;
using Amop.Core.Services.Http;
using Amop.Core.Services.Pond;
using Microsoft.Data.SqlClient;
using Polly;
using System.Data;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxPondGetDevices;

public class Function : AwsFunctionBase
{
    private int PageSize;
    // SQS Queue URL that is connected to the AltaworxPondGetDevices lambda
    private string GetDevicesQueueURL = string.Empty;
    private string ProcessStagedDevicesQueueURL = string.Empty;
    private string PondGetDeviceEndpoint = string.Empty;
    protected PondRepository pondRepository;
    protected ServiceProviderRepository serviceProviderRepository;
    protected SqsService sqsService = new SqsService();
    private readonly HttpRequestFactory _httpRequestFactory = new HttpRequestFactory();
    private readonly EnvironmentRepository _environmentRepo = new EnvironmentRepository();

    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        AmopLambdaContext? lambdaContext = null;
        try
        {
            lambdaContext = BaseAmopFunctionHandler(context);
            ArgumentNullException.ThrowIfNull(lambdaContext);

            InitializeRepositories(lambdaContext);

            TryGetAllEnvironmentVariables(lambdaContext);

            await ProcessEventAsync(lambdaContext, sqsEvent);
        }
        catch (Exception ex)
        {
            if (lambdaContext == null)
            {
                context.Logger.Log(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
            else
            {
                LogInfo(lambdaContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }

        base.CleanUp(lambdaContext);
    }

    protected void InitializeRepositories(AmopLambdaContext lambdaContext)
    {
        pondRepository = new PondRepository(lambdaContext.CentralDbConnectionString);
        serviceProviderRepository = new ServiceProviderRepository(lambdaContext.CentralDbConnectionString);
    }

    protected void TryGetAllEnvironmentVariables(AmopLambdaContext lambdaContext)
    {
        // Lambda related configurations
        GetDevicesQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_DEVICES_QUEUE_URL_VARIABLE_KEY);
        ProcessStagedDevicesQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_PROCESS_STAGED_DEVICES_QUEUE_URL_VARIABLE_KEY);
        // API related configurations
        PondGetDeviceEndpoint = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_DEVICE_ENDPOINT_VARIABLE_KEY);
        // Sync logic related configurations
        PageSize = GetIntValueFromEnvironmentVariable(lambdaContext, _environmentRepo,
            PondHelper.CommonString.PAGE_SIZE,
            PondHelper.CommonConfig.DEFAULT_PAGE_SIZE);
    }

    protected SqsValues GetMessageValues(AmopLambdaContext context, SQSEvent.SQSMessage message)
    {
        return new SqsValues(context, message);
    }

    protected async Task ProcessEventAsync(AmopLambdaContext context, SQSEvent sqsEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        if (sqsEvent?.Records != null)
        {
            var processedRecordCount = sqsEvent.Records.Count;
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
            foreach (var record in sqsEvent.Records)
            {
                LogInfo(context, CommonConstants.INFO, $"MessageId: {record.MessageId}");
                var sqsValues = GetMessageValues(context, record);
                if (sqsValues.ServiceProviderId <= 0)
                {
                    // No service provider id provided -> Initialize sync process using sqs message
                    await InitializeSyncDeviceProcess(context, serviceProviderRepository);
                }
                else
                {
                    // Run for the current service provider id (specified in the SQS Message)
                    await ProcessSyncPageByServiceProviderId(context, sqsValues);
                }
            }
        }
        else
        {
            await InitializeSyncDeviceProcess(context, serviceProviderRepository);
        }
    }

    protected async Task InitializeSyncDeviceProcess(AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository)
    {
        // Clean staging table
        var errorMessages = new List<string>();
        var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
        sqlTransientRetryPolicy.Execute(() => pondRepository.TruncateStagingTables(ParameterizedLog(context)));
        var serviceProviderIds = serviceProviderRepository.GetAllServiceProviderIds(ParameterizedLog(context), IntegrationType.Pond);
        if (serviceProviderIds?.Count > 0)
        {
            foreach (var serviceProviderId in serviceProviderIds)
            {
                var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, serviceProviderId);
                if (pondAuth == null)
                {
                    LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, serviceProviderId));
                    continue;
                }

                // Get all inventory ids
                var billingGroupIds = pondRepository.GetAllBillingGroupIds(ParameterizedLog(context), serviceProviderId);
                var pondApiService = new PondApiService(pondAuth, _httpRequestFactory, context.IsProduction);

                foreach (var billingGroupId in billingGroupIds)
                {
                    var formattedEndpoint = string.Format(PondGetDeviceEndpoint, billingGroupId);
                    // Call API once each inventory id to get total pages
                    var totalPages = await pondApiService.TryGetTotalPageCount<PondDeviceItem>(ParameterizedLog(context), formattedEndpoint, PageSize);
                    if (totalPages > 0)
                    {
                        LoadPagesToProcessTable(context, serviceProviderId, billingGroupId, totalPages);
                        // Page number start from 0 since the API need to be query by offset, which start by 0
                        // (first item of page = offset + pageNumber * pageSize)
                        for (var i = 0; i < totalPages; i++)
                        {
                            await InitGetDevicePages(context, serviceProviderId, billingGroupId, i);
                        }
                    }
                }
            }
        }
        else
        {
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NO_SERVICE_PROVIDER_FOUND, CommonConstants.POND_CARRIER_NAME));
        }
    }

    protected async Task ProcessSyncPageByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)
    {
        try
        {
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, sqsValues.ServiceProviderId);
            if (pondAuth == null)
            {
                LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, sqsValues.ServiceProviderId));
                return;
            }

            var pondApiService = new PondApiService(pondAuth, _httpRequestFactory, context.IsProduction);
            await SyncDevice(context, sqsValues, sqlTransientRetryPolicy, pondApiService);
        }
        catch (Exception ex)
        {
            LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
        }
    }

    protected async Task SyncDevice(AmopLambdaContext context, SqsValues sqsValues, ISyncPolicy syncPolicy, PondApiService pondApiService)
    {
        var formattedEndpoint = string.Format(PondGetDeviceEndpoint, sqsValues.BillingGroupId);
        await pondApiService.GetSinglePageListFromPondAPIAsync<PondDeviceItem, PondDeviceListResponse>(ParameterizedLog(context), syncPolicy, sqsValues.PageNumber, PageSize,
            (offset, pageSize) => pondApiService.GetPondListAsync<PondDeviceListResponse>(HttpClientSingleton.Instance, formattedEndpoint, offset, pageSize),
            (response) => response.Elements,
            (response) => LoadDevicesToStagingTable(context, response, sqsValues.ServiceProviderId),
            async (pageNumber, isSuccess) => await CheckSyncDeviceStepProgress(context, sqsValues.ServiceProviderId, sqsValues.BillingGroupId, pageNumber, isSuccess));
    }

    protected void LoadDevicesToStagingTable(AmopLambdaContext context, List<PondDeviceItem> pondDevices, int serviceProviderId)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondDeviceTable = new DataTable();
        pondDeviceTable.Columns.Add(CommonColumnNames.Id, typeof(int));
        pondDeviceTable.Columns.Add(CommonColumnNames.ICCID, typeof(string));
        pondDeviceTable.Columns.Add(CommonColumnNames.Parent_Group_Id, typeof(int));
        pondDeviceTable.Columns.Add(CommonColumnNames.Inventory_Id, typeof(int));
        pondDeviceTable.Columns.Add(CommonColumnNames.IMSI, typeof(string));
        pondDeviceTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));
        pondDeviceTable.Columns.Add(CommonColumnNames.CreatedDate, typeof(DateTime));

        foreach (var pondDeviceItem in pondDevices)
        {
            var pondDeviceRow = pondDeviceTable.NewRow();
            pondDeviceRow[CommonColumnNames.ICCID] = pondDeviceItem.ICCID;
            pondDeviceRow[CommonColumnNames.Parent_Group_Id] = pondDeviceItem.ParentGroupId;
            pondDeviceRow[CommonColumnNames.Inventory_Id] = pondDeviceItem.InventoryId;
            pondDeviceRow[CommonColumnNames.IMSI] = pondDeviceItem.IMSIs.Select(x => x.IMSINumber.ToString()).FirstOrDefault();
            pondDeviceRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondDeviceRow[CommonColumnNames.CreatedDate] = DateTime.UtcNow;
            pondDeviceTable.Rows.Add(pondDeviceRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondDeviceTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondDeviceTable, DatabaseTableNames.PondDeviceStaging, columnMappings);
    }

    protected async Task CheckSyncDeviceStepProgress(AmopLambdaContext context, int serviceProviderId, int inventoryId, int currentPage, bool isSuccess)
    {
        var attributes = new Dictionary<string, string>()
        {
            {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, serviceProviderId.ToString()},
            {SQSMessageKeyConstant.BILLING_GROUP_ID, inventoryId.ToString()},
            {SQSMessageKeyConstant.PAGE_NUMBER, currentPage.ToString()},
            {SQSMessageKeyConstant.IS_SUCCESSFUL, isSuccess.ToString()},
        };

        await sqsService.SendSQSMessage(ParameterizedLog(context), AwsCredentials(context), ProcessStagedDevicesQueueURL, attributes);
    }

    protected static void LoadPagesToProcessTable(AmopLambdaContext context, int serviceProviderId, int billingGroupId, int totalPages)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondDevicesPageTable = new DataTable();
        pondDevicesPageTable.Columns.Add(CommonColumnNames.PageNumber, typeof(int));
        pondDevicesPageTable.Columns.Add(CommonColumnNames.BillingGroupId, typeof(int));
        pondDevicesPageTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));

        for (var i = 0; i < totalPages; i++)
        {
            var pondDevicesPageRow = pondDevicesPageTable.NewRow();
            pondDevicesPageRow[CommonColumnNames.PageNumber] = i;
            pondDevicesPageRow[CommonColumnNames.BillingGroupId] = billingGroupId;
            pondDevicesPageRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondDevicesPageTable.Rows.Add(pondDevicesPageRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondDevicesPageTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondDevicesPageTable, DatabaseTableNames.POND_GET_DEVICES_PAGE_TO_PROCESS, columnMappings);
    }

    protected async Task InitGetDevicePages(AmopLambdaContext context, int serviceProviderId, int billingGroupId, int currentPage)
    {
        // Insert all the pages to table
        var attributes = new Dictionary<string, string>()
        {
            {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, serviceProviderId.ToString()},
            {SQSMessageKeyConstant.BILLING_GROUP_ID, billingGroupId.ToString()},
            {SQSMessageKeyConstant.PAGE_NUMBER, currentPage.ToString()},
        };
        await sqsService.SendSQSMessage(ParameterizedLog(context), AwsCredentials(context), GetDevicesQueueURL, attributes);
    }

}