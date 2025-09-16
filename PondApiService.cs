using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Helpers.Teal;
using Amop.Core.Logger;
using Amop.Core.Models.DeviceBulkChange;
using Amop.Core.Models.Pond;
using Amop.Core.Repositories.Pond;
using Amop.Core.Services.Base64Service;
using Amop.Core.Services.Http;
using Newtonsoft.Json;
using Polly;

namespace Amop.Core.Services.Pond
{
    public class PondApiService
    {
        private readonly PondAuthentication _pondAuthentication;
        private readonly IHttpRequestFactory _httpRequestFactory;
        private readonly bool _isProduction;

        public PondApiService(PondAuthentication PondAuthentication, IHttpRequestFactory httpRequestFactory, bool isProduction)
        {
            _pondAuthentication = PondAuthentication;
            _httpRequestFactory = httpRequestFactory;
            _isProduction = isProduction;
        }

        // Default page size from API documentation is 10
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

        public async Task<PondGetDeviceStatusResponse> GetDeviceStatus(HttpClient httpClient, string ICCID, IKeysysLogger logger = null)
        {
            var baseUri = _isProduction ? _pondAuthentication.ProductionURL : _pondAuthentication.SandboxURL;
            logger?.LogInfo(CommonConstants.SUB, $"({ICCID}, {URLConstants.POND_DEVICE_STATUS_END_POINT})");
            var apiUrl = $"{baseUri.TrimEnd('/')}/{string.Format(URLConstants.POND_DEVICE_STATUS_END_POINT, _pondAuthentication.DistributorId, ICCID)}";

            var requestMessage = BuildRequestMessage(apiUrl, CommonConstants.METHOD_GET);
            var response = await httpClient.SendAsync(requestMessage);
            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                try
                {
                    var pondDeviceStatusResponse = JsonConvert.DeserializeObject<PondGetDeviceStatusResponse>(responseBody);
                    return pondDeviceStatusResponse;
                }
                catch (Exception ex)
                {
                    logger?.LogInfo(CommonConstants.EXCEPTION, ex.Message);
                    return null;
                }
            }
            logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.RESPONSE_ERROR_MESSAGE, responseBody));
            return null;
        }

        public async Task<DeviceChangeResult<string, string>> ProcessPondUpdateAsync(HttpClient httpClient, string request, string apiUrl, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({apiUrl})");

            var requestContent = new StringContent(request, Encoding.UTF8, CommonConstants.APPLICATION_JSON);
            var response = await RetryPolicyHelper.PollyRetryHttpRequestAsync(logger, PondHelper.CommonConfig.RETRY_NUMBER)
                .ExecuteAsync(async () =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.REQUEST_LOG_TEMPLATE, CommonConstants.METHOD_POST, apiUrl));
                    var requestMessage = _httpRequestFactory.BuildRequestMessage(_pondAuthentication, new HttpMethod(CommonConstants.METHOD_POST), new Uri(apiUrl), BuildRequestHeader(_pondAuthentication.APIKey), requestContent);
                    return await httpClient.SendAsync(requestMessage);
                });

            var responseBody = await response.Content.ReadAsStringAsync();
            if (response.IsSuccessStatusCode)
            {
                return BuildAPIResult(CommonConstants.METHOD_POST, apiUrl, responseBody, false, request);
            }
            logger?.LogInfo(CommonConstants.EXCEPTION, LogCommonStrings.ERROR_WHILE_CALLING_TEAL_API);
            return BuildAPIResult(CommonConstants.METHOD_POST, apiUrl, LogCommonStrings.ERROR_WHILE_CALLING_POND_API, true, request);
        }

        public async Task<DeviceChangeResult<string, string>> UpdatePackageStatus(PondAuthentication pondAuthentication, string baseUri, string packageId, string newPackageStatus)
        {
            var pondUpdatePackageStatusApiUrl = $"{baseUri.TrimEnd('/')}/{pondAuthentication.DistributorId}/{string.Format(URLConstants.POND_UPDATE_PACKAGE_STATUS_END_POINT, packageId)}";
            var pondUpdatePackageStatus = new PondUpdatePackageStatus()
            {
                PackageStatus = newPackageStatus
            };
            var pondUpdatePackageStatusRequest = JsonConvert.SerializeObject(pondUpdatePackageStatus);
            var pondUpdatePackageResponse = await ProcessPondUpdateAsync(HttpClientSingleton.Instance, pondUpdatePackageStatusRequest, pondUpdatePackageStatusApiUrl, null);
            return pondUpdatePackageResponse;
        }

        private DeviceChangeResult<string, string> BuildAPIResult(string httpRequestType, string baseAddress, string responseBody, bool hasError, string requestText = null)
        {
            if (string.IsNullOrWhiteSpace(responseBody))
            {
                responseBody = CommonConstants.OK;
            }
            return new DeviceChangeResult<string, string>()
            {
                ActionText = $"{httpRequestType} {baseAddress}",
                HasErrors = hasError,
                RequestObject = requestText,
                ResponseObject = responseBody
            };
        }

        public async Task<DeviceChangeResult<string, string>> UpdateServiceStatus(HttpClient httpClient, string ICCID, PondUpdateServiceStatusRequest request, IKeysysLogger logger = null)
        {
            var baseUri = _isProduction ? _pondAuthentication.ProductionURL : _pondAuthentication.SandboxURL;
            logger?.LogInfo(CommonConstants.SUB, $"({ICCID}, {URLConstants.POND_DEVICE_STATUS_END_POINT})");
            var apiUrl = $"{baseUri.TrimEnd('/')}/{string.Format(URLConstants.POND_DEVICE_STATUS_END_POINT, _pondAuthentication.DistributorId, ICCID)}";
            var jsonRequest = JsonConvert.SerializeObject(request);

            var requestMessage = BuildRequestMessage(apiUrl, CommonConstants.METHOD_POST, jsonRequest);
            var response = await httpClient.SendAsync(requestMessage);
            var responseBody = await response.Content.ReadAsStringAsync();
            var actionText = $"POST {apiUrl}";

            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, string>
                {
                    ActionText = actionText,
                    ResponseObject = CommonConstants.RESPONSE_OK,
                    HasErrors = false,
                    RequestObject = jsonRequest
                };
            }
            logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.RESPONSE_ERROR_MESSAGE, responseBody));
            return new DeviceChangeResult<string, string>
            {
                ActionText = actionText,
                ResponseObject = responseBody,
                HasErrors = true,
                RequestObject = jsonRequest
            };
        }

        public async Task<DeviceChangeResult<string, string>> PurgeDevice(HttpClient httpClient, string ICCID, IKeysysLogger logger = null)
        {
            logger?.LogInfo(CommonConstants.SUB, $"({ICCID}, {URLConstants.POND_PURGE_DEVICE_END_POINT})");
            var apiUrl = $"{string.Format(URLConstants.POND_PURGE_DEVICE_END_POINT, ICCID)}";

            // Change method if needed
            var requestMessage = BuildRequestMessage(apiUrl, CommonConstants.METHOD_DELETE, null, _pondAuthentication.TokenValue);
            var response = await httpClient.SendAsync(requestMessage);
            var responseBody = await response.Content.ReadAsStringAsync();
            var actionText = $"GET {apiUrl}";

            if (response.IsSuccessStatusCode)
            {
                return new DeviceChangeResult<string, string>
                {
                    ActionText = actionText,
                    ResponseObject = CommonConstants.RESPONSE_OK,
                    HasErrors = false,
                    RequestObject = string.Empty
                };
            }
            logger?.LogInfo(CommonConstants.ERROR, string.Format(LogCommonStrings.RESPONSE_ERROR_MESSAGE, responseBody));
            return new DeviceChangeResult<string, string>
            {
                ActionText = actionText,
                ResponseObject = responseBody,
                HasErrors = true,
                RequestObject = string.Empty
            };
        }

        public static Dictionary<string, string> BuildQueryParamGetInventoryList(int offset, int pageSize)
        {
            return new Dictionary<string, string>
            {
                { PondHelper.CommonString.OFFSET, offset.ToString() },
                { PondHelper.CommonString.COUNT, pageSize.ToString() }
            };
        }

        public async Task GetSinglePageListFromPondAPIAsync<TListItem, TAPIResponse>(Action<string, string> logFunction, ISyncPolicy sqlRetryPolicy, int pageNumber, int pageSize,
            Func<int, int, Task<TAPIResponse>> getFromAPIFunc,
            Func<TAPIResponse, List<TListItem>> getListFromResponseFunc,
            Action<List<TListItem>> loadDataToStagingFunc,
            Func<int, bool, Task> checkSyncStepProgressFunc)
        {
            logFunction(CommonConstants.SUB, string.Empty);

            List<TListItem> elements = new List<TListItem>();
            var apiResponse = await getFromAPIFunc(pageNumber * pageSize, pageSize);
            bool isSuccess = false;
            if (apiResponse != null)
            {
                var results = getListFromResponseFunc(apiResponse);
                if (results != null)
                {
                    elements.AddRange(results);
                    isSuccess = true;
                }
            }
            else
            {
                logFunction(CommonConstants.EXCEPTION, LogCommonStrings.ERROR_NULL_API_REPONSE);
            }

            sqlRetryPolicy.Execute(() => loadDataToStagingFunc(elements));
            await checkSyncStepProgressFunc(pageNumber, isSuccess);
        }

        public async Task<int> TryGetTotalPageCount<T>(Action<string, string> logFunction, string endpoint, int pageSize)
        {
            try
            {
                var firstPageResult = await GetPondListAsync<PondBaseListResponse<T>>(HttpClientSingleton.Instance, endpoint, 0, pageSize);
                return (int)Math.Ceiling((double)firstPageResult.Total / pageSize);
            }
            catch (Exception ex)
            {
                logFunction(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
                return 0;
            }
        }

        private Dictionary<string, string> BuildRequestHeader(string apiKey)
        {
            return new Dictionary<string, string> {
                { PondHelper.CommonString.APPLICATION_ACCEPTED, CommonConstants.APPLICATION_JSON },
                { PondHelper.CommonString.API_KEY, apiKey}
            };
        }

        public async Task GetListFromPondAPIAsync<TListItem, TAPIResponse>(IKeysysLogger logger, int maxPagesPerLambda, ISyncPolicy sqlRetryPolicy, int pageNumber, int pageSize,
            Func<int, int, Task<TAPIResponse>> getFromAPIFunc,
            Func<TAPIResponse, bool> checkIsLastPageFunc,
            Func<TAPIResponse, List<TListItem>> getListFromResponseFunc,
            Action<List<TListItem>> loadDataToStagingFunc,
            Action<int, bool> checkSyncStepProgressFunc = null,
            ILambdaContext lambdaContext = null)
        {
            logger.LogInfo(CommonConstants.SUB, string.Empty);

            List<TListItem> elements = new List<TListItem>();
            bool isLastPage = false;
            bool isError = false;
            var remainingTime = lambdaContext?.RemainingTime.TotalSeconds ?? CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES;
            while (!isLastPage && pageNumber <= maxPagesPerLambda && remainingTime > PondHelper.CommonConfig.DEVICE_RATE_PLAN_SYNC_REMAINING_SECONDS_LIMIT)
            {
                try
                {
                    var apiResponse = await getFromAPIFunc(pageNumber * pageSize, pageSize);
                    if (apiResponse != null)
                    {
                        isLastPage = checkIsLastPageFunc(apiResponse);
                        if (isLastPage && getListFromResponseFunc(apiResponse) != null)
                        {
                            elements.AddRange(getListFromResponseFunc(apiResponse));
                            pageNumber++;
                        }
                    }
                    else
                    {
                        // Stop if error
                        isError = true;
                        logger.LogInfo(CommonConstants.EXCEPTION, LogCommonStrings.ERROR_NULL_API_REPONSE);
                        break;
                    }
                }
                catch (Exception ex)
                {
                    // Stop if error
                    isError = true;
                    logger.LogInfo(CommonConstants.EXCEPTION, $"{ex.Message} - {ex.StackTrace}");
                    break;
                }
                remainingTime = lambdaContext?.RemainingTime.TotalSeconds ?? CommonConstants.DELAY_IN_SECONDS_FIFTEEN_MINUTES;
                logger.LogInfo(CommonConstants.INFO, $"{nameof(remainingTime)}: {remainingTime} seconds");
                logger.LogInfo(CommonConstants.INFO, $"{nameof(pageNumber)}: {pageNumber}");
            }

            sqlRetryPolicy.Execute(() => loadDataToStagingFunc(elements));
            if (checkSyncStepProgressFunc != null && !isError)
            {
                checkSyncStepProgressFunc(pageNumber, isLastPage);
            }
        }

        private HttpRequestMessage BuildRequestMessage(string baseURL, string method, string content = null, string tokenValue = null)
        {
            if (string.IsNullOrWhiteSpace(content))
            {
                if (!string.IsNullOrWhiteSpace(tokenValue))
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

    }
}
