### AltaworxPondGetDevices Lambda Flow Documentation

## Overview
The `AltaworxPondGetDevices` Lambda function synchronizes device data from the Pond API into the database. It can initialize a device sync session (seeding page work) and then process device pages in batches via SQS messages.

## HIGH-LEVEL FLOW (Sequential Function Flow)

### Main Entry Point
- **FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)**
  - Receives SQS event and Lambda context
  - Initializes base function handler
  - Iterates through SQS records and routes per-message

### Initialization Flow (ServiceProviderId not supplied in SQS message)
- **InitializeSyncDeviceProcess**
  - TruncateDeviceStagingTables (staging reset)
  - GetAllServiceProviderIds(IntegrationType.Pond)
  - For each Service Provider (SP):
    - GetPondAuthentication
    - TryGetTotalPageCount via Pond API (PondGetDeviceEndpoint, PageSize)
    - LoadDevicePagesToProcessTable (seed page markers)
    - InitGetDevicePages (enqueue one SQS message per page)
    - Shape

### Processing Flow (ServiceProviderId supplied in SQS message)
- **ProcessSyncDevicePageByServiceProviderId**
  - GetPondAuthentication
  - Instantiate PondApiService
  - SyncDevices
    - GetSinglePageDeviceListFromPondAPIAsync (paged fetch)
    - LoadDevicesToStagingTable (bulk copy to staging)
    - CheckSyncDeviceStepProgress (emit progress/completion message)
    - Shape

## LOW-LEVEL FLOW (Detailed Method Explanations)

### FunctionHandler (Main Entry Point)
- **Input**: `SQSEvent sqsEvent`, `ILambdaContext context`
- **Purpose**: Processes SQS messages to orchestrate device synchronization
- **What happens**:
  - Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`
  - Reads environment variables via `TryGetAllEnvironmentVariables()`:
    - `POND_GET_DEVICES_QUEUE_URL` (`PondHelper.CommonString.POND_GET_DEVICES_QUEUE_URL_VARIABLE_KEY`)
    - `POND_PROCESS_STAGED_DEVICES_QUEUE_URL` (`PondHelper.CommonString.POND_PROCESS_STAGED_DEVICES_QUEUE_URL_VARIABLE_KEY`)
    - `POND_GET_DEVICE_ENDPOINT` (`PondHelper.CommonString.POND_GET_DEVICE_ENDPOINT_VARIABLE_KEY`)
    - `PAGE_SIZE` (`PondHelper.CommonString.PAGE_SIZE`, default `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)
  - Ensures SQS trigger validity and iterates each record
  - For each record:
    - Logs diagnostics
    - Parses attributes with `GetMessageValues()`:
      - `ServiceProviderId` (required for processing mode)
      - `PageNumber` (required for processing mode)
      - `IsSuccessful` (used in downstream stage processing queue)
    - If `ServiceProviderId` <= 0 or missing: routes to `InitializeSyncDeviceProcess()`
    - Else: routes to `ProcessSyncDevicePageByServiceProviderId()`
  - Handles exceptions and calls `CleanUp()`

### InitializeSyncDeviceProcess (Initialization Mode)
- **Input**: `AmopLambdaContext context`, `ServiceProviderRepository serviceProviderRepository`
- **Purpose**: Seeds the sync process and fans out processing across pages
- **What happens**:
  - Reset staging via `pondRepository.TruncateDeviceStagingTables`
  - Retrieve all Pond service provider IDs via `GetAllServiceProviderIds`
  - For each `serviceProviderId`:
    - Retrieve auth via `pondRepository.GetPondAuthentication`
    - Call API once to get total page count via `pondApiService.TryGetTotalPageCount<T>` using `PondGetDeviceEndpoint`
    - `LoadDevicePagesToProcessTable(context, serviceProviderId, totalPages)` seeds DB page markers (`POND_GET_DEVICES_PAGE_TO_PROCESS`)
    - For page in [0, totalPages):
      - `InitGetDevicePages(context, serviceProviderId, page)` enqueues SQS message to `GetDevicesQueueURL`
    - Shape

### ProcessSyncDevicePageByServiceProviderId (Processing Mode)
- **Input**: `AmopLambdaContext context`, `SqsValues sqsValues`
- **Purpose**: Pulls one page of devices from Pond and loads to staging
- **What happens**:
  - Retrieve auth via `pondRepository.GetPondAuthentication`
  - Create `PondApiService`
  - `SyncDevices(context, sqsValues, sqlTransientRetryPolicy, pondApiService)`:
    - Calls `GetSinglePageDeviceListFromPondAPIAsync<PondDeviceItem, PondDeviceListResponse>`:
      - Calculates `offset = pageNumber * PageSize`
      - Fetches from Pond via `GetPondListAsync<PondDeviceListResponse>(HttpClientSingleton.Instance, PondGetDeviceEndpoint, offset, PageSize)`
      - Extracts list via `response => response.Elements`
    - `LoadDevicesToStagingTable` builds a `DataTable` and executes `SqlBulkCopy` to `PondDeviceStaging`
    - On each page, calls `CheckSyncDeviceStepProgress` with `IsSuccessful`, which emits an SQS message to `ProcessStagedDevicesQueueURL` for downstream processing
    - Shape

## Utility Functions

- **GetMessageValues**
  - Parses SQS attributes from the incoming message into `SqsValues`
  - Attributes used (via `SQSMessageKeyConstant`):
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL` (used for progress/completion signaling downstream)

- **TryGetAllEnvironmentVariables**
  - Reads Lambda, API, and sync configuration from environment variables:
    - `POND_GET_DEVICES_QUEUE_URL`
    - `POND_PROCESS_STAGED_DEVICES_QUEUE_URL`
    - `POND_GET_DEVICE_ENDPOINT`
    - `PAGE_SIZE`

- **InitializeRepositories**
  - Instantiates `PondRepository` and `ServiceProviderRepository` using `CentralDbConnectionString`

- **LoadDevicesToStagingTable**
  - Shapes `DataTable` schema with columns: `Id`, `Iccid`, `Imei`, `Msisdn`, `Status`, `CreatedDate`, `ServiceProviderId`
  - Executes `SqlBulkCopy` into `DatabaseTableNames.PondDeviceStaging`

- **LoadDevicePagesToProcessTable**
  - Builds `DataTable` with `PageNumber` and `ServiceProviderId` for all pages
  - Executes `SqlBulkCopy` into `DatabaseTableNames.POND_GET_DEVICES_PAGE_TO_PROCESS`

- **InitGetDevicePages**
  - Sends an SQS message per page to `GetDevicesQueueURL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`

- **CheckSyncDeviceStepProgress**
  - Sends an SQS message to `ProcessStagedDevicesQueueURL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL`

- **SyncDevices**
  - Orchestrates single-page fetch, staging load, and progress signaling using retry policy

## Key Dependencies and Integrations
- **AwsFunctionBase**: logging, config, DB connections, bulk copy, cleanup
- **PondRepository**: DB CRUD for Pond sync, staging, and progress tracking
- **PondApiService**: list API calls, query param construction, request building
- **ServiceProviderRepository**: service provider enumeration and metadata
- **EnvironmentRepository**: environment variable access
- **SqsService**: SQS message publishing
- **RetryPolicyHelper**: SQL transient retry policy
- **HttpClientSingleton** and **HttpRequestFactory**: HTTP client and request construction
- Shape

## Data Flow Summary
- **Initialization**: seed page markers per service provider and enqueue SQS messages per page
- **Fetch**: pull one page of devices from Pond using `offset = pageNumber * pageSize`
- **Stage**: bulk insert to `PondDeviceStaging`
- **Advance**: emit progress messages to `ProcessStagedDevicesQueueURL` for downstream processing
- **Note**: Page-to-process tracking is staged into `POND_GET_DEVICES_PAGE_TO_PROCESS`; downstream components can update progress via repository methods (e.g., `UpdateDevicesPageStatusAndCheckSyncProgress`) as applicable
- Shape

## URLs and Credentials
- **BaseUrl**: `https://www.mydashboard.pondmobile.com/`
- **ProductionURL**: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
- **SandboxURL**: `https://www.mydashboard.pondmobile.com/ds/u/distributorPPUService/v1`
- **APIKey**: `8de5bfa4-8c8f-4495-85ab-c90d6b0d1ca7`
- **Username**: `person@altaworx.com`
- **EncodedPassword**: `M2YxMjUzNzYtNzljZi00N2VlLTk4NTEtNjQyY2MyZWVjNmU4`
- **TokenValue**: `eyJvcmciOiI2Mjg2MWUxZmY4YjU3ZDAwMDEzNmI1NjkiLCJpZCI6IjU1M2MzYWUwMGU3NjRlMjM4MzYxOWY3OWY4N2I3YWZlIiwiaCI6Im11cm11cjY0In0`

## Shared API Call Helpers (used unchanged for Devices)

```csharp
public async Task<T> GetPondListAsync<T>(HttpClient httpClient, string endpoint, int offset = 0, int pageSize = PondHelper.CommonConfig.DEFAULT_PAGE_SIZE, IKeysysLogger logger = null)
{
    logger?.LogInfo(CommonConstants.SUB, $"({endpoint}, {offset}, {pageSize})");

    var baseUri = _isProduction ? _pondAuthentication.ProductionURL : _pondAuthentication.SandboxURL;
    var queryParameters = BuildQueryParamGetInventoryList(offset, pageSize);
    var dictFormUrlEncoded = new FormUrlEncodedContent(queryParameters);
    var queryString = await dictFormUrlEncoded.ReadAsStringAsync();
    var apiUrl = $"{baseUri.TrimEnd('/')}/{_pondAuthentication.DistributorId}/{endpoint}?{queryString}";

    var requestMessage = BuildRequestMessage(apiUrl, CommonConstants.METHOD_GET);

    var response = await httpClient.SendAsync(requestMessage);
    var responseBody = await response.Content.ReadAsStringAsync();

    if (!response.IsSuccessStatusCode)
    {
        logger?.LogError(CommonConstants.ERROR, responseBody);
    }
    return JsonConvert.DeserializeObject<T>(responseBody);
}
```

```csharp
private HttpRequestMessage BuildRequestMessage(string baseURL, string method, string content = null, string tokenValue = null)
{
    if (string.IsNullOrWhiteSpace(content))
    {
        if(!string.IsNullOrWhiteSpace(tokenValue))
        {
            return _httpRequestFactory.BuildRequestMessage(
            _pondAuthentication,
            new HttpMethod(method),
            new Uri(baseURL),
            BuildRequestHeader(_pondAuthentication.APIKey),
            null,
            tokenValue
            );
        }
        return _httpRequestFactory.BuildRequestMessage(
            _pondAuthentication,
            new HttpMethod(method),
            new Uri(baseURL),
            BuildRequestHeader(_pondAuthentication.APIKey)
        );
    }

    var requestContent = new StringContent(content, Encoding.UTF8, CommonConstants.APPLICATION_JSON);

    return _httpRequestFactory.BuildRequestMessage(
        _pondAuthentication,
        new HttpMethod(method),
        new Uri(baseURL),
        BuildRequestHeader(_pondAuthentication.APIKey),
        requestContent
    );
}
```

```csharp
private Dictionary<string, string> BuildRequestHeader(string apiKey)
{
    return new Dictionary<string, string> {
        { PondHelper.CommonString.APPLICATION_ACCEPTED, CommonConstants.APPLICATION_JSON },
        { PondHelper.CommonString.API_KEY, apiKey}
    };
}
```

## Example Stored Procedure (Devices)

```sql
CREATE PROCEDURE [dbo].[usp_Pond_GetAllDeviceIds]
    @ServiceProviderId INT
AS
BEGIN
    SELECT [Id]
    FROM [dbo].[PondDevice]
    WHERE [IsActive] = 1
      AND [IsDeleted] = 0
      AND [ServiceProviderId] = @ServiceProviderId;
END;
```

