using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Amop.Core.Constants;
using Amop.Core.Logger;
using Amop.Core.Models;
using Amop.Core.Models.Revio;
using Amop.Core.Resilience;
using Amop.Core.Services.RegexService;
using Newtonsoft.Json;
using Polly;
using Polly.Wrap;
using System.Linq;
using Polly.Retry;
using System.Net;
using Amazon.Runtime;

namespace Amop.Core.Helpers
{
    public static class RetryPolicyHelper
    {
        public const int SQL_TRANSIENT_RETRY_MAX_COUNT = 3;
        private const string EXCEPTION_MESSAGE_DEFAULT = "UNKNOWN ERROR";

        public static ISyncPolicy GetSqlTransientPolicy(IKeysysLogger logger, List<string> errorMessages, int retryCount = SQL_TRANSIENT_RETRY_MAX_COUNT)
        {
            var fallbackPolicy = GetFallbackPolicy(errorMessages);
            return fallbackPolicy.Wrap(GetSqlPolicy(logger, retryCount));
        }

        public static AsyncPolicyWrap<HttpResponseMessage> PollyRetryHttpRequestAsync(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .Or<TimeoutException>()
                .OrResult<HttpResponseMessage>(x => !x.IsSuccessStatusCode)
                .WaitAndRetryAsync(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.RETRY_LOG_FORMAT, retryCount, numberOfRetry, waitTime.TotalSeconds));
                });

            var fallbackPolicy = Policy
                .Handle<Exception>()
                .OrResult<HttpResponseMessage>(x => !x.IsSuccessStatusCode)
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        //If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        //In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest)
                            {
                                Content = new StringContent(exception.Message)
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }
        public static AsyncPolicyWrap<ProxyResultBase> PollyRetryForProxyRequestAsync(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .Or<TimeoutException>()
                .OrResult<ProxyResultBase>(x => !x.IsSuccessful)
                .WaitAndRetryAsync(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logger?.LogInfo(CommonConstants.INFO, $"Retry number: {retryCount}. Wait {waitTime.TotalSeconds} seconds.");
                });

            var fallbackPolicy = Policy
                .Handle<Exception>()
                .OrResult<ProxyResultBase>(x => !x.IsSuccessful)
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        //If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        //In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new PostProxyResult()
                            {
                                IsSuccessful = false,
                                ResponseMessage = response.Result?.ResponseMessage,
                                StatusCode = response.Result?.StatusCode
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }

        public static AsyncPolicyWrap<HttpResponseMessage> PollyRetryRevIOHttpRequestAsync(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = BaseRetryRevIOHttpRequestAsync(logger, numberOfRetry);

            var fallbackPolicy = Policy
                .Handle<Exception>()
                .OrResult<HttpResponseMessage>(x => !x.IsSuccessStatusCode)
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        //If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        //In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest)
                            {
                                Content = new StringContent(exception.Message)
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }

        private static AsyncRetryPolicy<HttpResponseMessage> BaseRetryRevIOHttpRequestAsync(IKeysysLogger logger, int numberOfRetry)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .Or<TimeoutException>()
                .OrResult<HttpResponseMessage>(x => !x.IsSuccessStatusCode && x.StatusCode != HttpStatusCode.InternalServerError)
                .WaitAndRetryAsync(
                    retryCount: numberOfRetry,
                    sleepDurationProvider: (retryAttempt, retryResponse, context) =>
                    {
                        // Too many request
                        // HttpStatusCode enum does not have 429 so we create our own constant
                        if (retryResponse.Result != null && (int)retryResponse.Result.StatusCode == CommonConstants.TOO_MANY_REQUEST_HTTP_STATUS_CODE)
                        {
                            try
                            {
                                // Parse the response for the seconds
                                var responseBody = retryResponse.Result.Content.ReadAsStringAsync().Result;
                                var revIOError = JsonConvert.DeserializeObject<RevIoErrorResponse>(responseBody);
                                logger?.LogInfo(CommonConstants.INFO, $"Response: {revIOError}");

                                var regexService = new RegexService();
                                var secondsString = regexService.GetFirstNumberFromText(revIOError.Message).Value;
                                if (string.IsNullOrWhiteSpace(secondsString))
                                {
                                    // Fail to get string value from message
                                    return FallbackParseErrorRetryWaitTime(logger, numberOfRetry, retryAttempt);
                                }
                                var waitSeconds = int.Parse(secondsString) + 1;

                                logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.WAIT_TIME_AFTER_TOO_MANY_REQUESTS, waitSeconds));
                                return TimeSpan.FromSeconds(waitSeconds);
                            }
                            catch (Exception)
                            {
                                // Fail to parse or the error JSON format have been change 
                                return FallbackParseErrorRetryWaitTime(logger, numberOfRetry, retryAttempt);
                            }
                        }
                        else
                        {
                            return CalculateRetryDelay(retryAttempt);
                        }
                    },
                    onRetryAsync: (response, waitTime, retryCount, context) =>
                    {
                        logger?.LogInfo(CommonConstants.INFO, $"Retry number: {retryCount} of {numberOfRetry}. Wait {waitTime.TotalSeconds} seconds.");
                        return Task.CompletedTask;
                    });

            return retryPolicy;
        }

        private static TimeSpan FallbackParseErrorRetryWaitTime(IKeysysLogger logger, int numberOfRetry, int retryAttempt)
        {
            var waitTime = CalculateRetryDelay(retryAttempt);

            logger?.LogInfo(CommonConstants.WARNING, string.Format(LogCommonStrings.ERROR_WHEN_PARSING_IO_RESPONSE_FOR_RETRY_LOGIC, retryAttempt, numberOfRetry, waitTime.TotalSeconds));
            return waitTime;
        }

        public static TimeSpan CalculateRetryDelay(int retryAttempt)
        {
            return TimeSpan.FromSeconds(Math.Pow(CommonConstants.API_ERROR_DELAY_IN_SECONDS, retryAttempt));
        }

        private static ISyncPolicy GetFallbackPolicy(List<string> errorMessages)
        {
            return Policy
                .Handle<Exception>()
                .Fallback(
                    (exception, context, token) => { },
                    (exception, context) => { errorMessages.Add(exception?.Message ?? EXCEPTION_MESSAGE_DEFAULT); }
                );
        }

        private static ISyncPolicy GetSqlPolicy(IKeysysLogger logger, int retryCount)
        {
            var policyFactory = new PolicyFactory(logger);
            return policyFactory.GetSqlRetryPolicy(retryCount);
        }

        public static AsyncPolicyWrap<ProxyResultBase> PollyRetryProxyRequestForInternalServerErrorAsync(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .Or<TimeoutException>()
                .OrResult<ProxyResultBase>(x => x.StatusCode == CommonConstants.STATUS_CODE_FOR_INTERNAL_SERVER_ERROR || x.StatusCode == HttpStatusCode.InternalServerError.ToString())
                .WaitAndRetryAsync(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logger?.LogInfo(CommonConstants.INFO, $"Retry number: {retryCount}. Wait {waitTime.TotalSeconds} seconds.");
                });

            var fallbackPolicy = Policy
                .Handle<Exception>()
                .OrResult<ProxyResultBase>(x => x.StatusCode == CommonConstants.STATUS_CODE_FOR_INTERNAL_SERVER_ERROR || x.StatusCode == HttpStatusCode.InternalServerError.ToString())
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        // If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        // In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new PostProxyResult()
                            {
                                IsSuccessful = false,
                                ResponseMessage = response.Result?.ResponseMessage,
                                StatusCode = response.Result?.StatusCode
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }

        public static AsyncPolicyWrap<HttpResponseBase> PollyRetryHttpRequestForInternalServerErrorAsync(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .Or<TimeoutException>()
                .OrResult<HttpResponseBase>(x => x.StatusCode == CommonConstants.STATUS_CODE_FOR_INTERNAL_SERVER_ERROR || x.StatusCode == HttpStatusCode.InternalServerError.ToString())
                .WaitAndRetryAsync(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logger?.LogInfo(CommonConstants.INFO, $"Retry number: {retryCount}. Wait {waitTime.TotalSeconds} seconds.");
                });

            var fallbackPolicy = Policy
                .Handle<Exception>()
                .OrResult<HttpResponseBase>(x => x.StatusCode == CommonConstants.STATUS_CODE_FOR_INTERNAL_SERVER_ERROR || x.StatusCode == HttpStatusCode.InternalServerError.ToString())
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        // If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        // In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new HttpResponseBase()
                            {
                                HeaderContent = response.Result?.HeaderContent,
                                IsSuccessful = false,
                                ResponseMessage = exception.Message,
                                StatusCode = response.Result?.StatusCode
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }


        public static AsyncPolicyWrap<AmazonWebServiceResponse> PollyRetryForSQSMessage(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .OrResult<AmazonWebServiceResponse>(x => x.HttpStatusCode == HttpStatusCode.InternalServerError)
                .WaitAndRetryAsync(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.RETRY_LOG_FORMAT, retryCount, numberOfRetry, waitTime.TotalSeconds));
                });

            var fallbackPolicy = Policy
                .Handle<Exception>()
                // Retry on any 4xx or 5xx response statuses
                .OrResult<AmazonWebServiceResponse>(x => x.HttpStatusCode >= HttpStatusCode.BadRequest)
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        // If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        // In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new AmazonWebServiceResponse()
                            {
                                HttpStatusCode = HttpStatusCode.InternalServerError
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }

        public static AsyncPolicyWrap<HttpResponseBase> PollyRetryHttpRequestResponseBaseAsync(IKeysysLogger logger = null, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .Or<TimeoutException>()
                .OrResult<HttpResponseBase>(x => !x.IsSuccessful)
                .WaitAndRetryAsync(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logger?.LogInfo(CommonConstants.INFO, string.Format(LogCommonStrings.RETRY_LOG_FORMAT, retryCount, numberOfRetry, waitTime.TotalSeconds));
                });

            var fallbackPolicy = Policy
                .Handle<Exception>()
                .OrResult<HttpResponseBase>(x => !x.IsSuccessful)
                .FallbackAsync(
                    async (response, context, token) =>
                    {
                        var exception = response.Exception;
                        // If the response.Exception has a value when calling the API, it means that an exception occurred while attempting to receive the response from the API.
                        // In this case, the response.Result will always be null.
                        if (exception != null)
                        {
                            logger?.LogInfo(CommonConstants.EXCEPTION, $"Message: {exception.Message}. Stack trace: {exception.StackTrace}");
                            var customResponse = new HttpResponseBase()
                            {
                                HeaderContent = response.Result?.HeaderContent,
                                IsSuccessful = false,
                                ResponseMessage = exception.Message,
                                StatusCode = response.Result?.StatusCode
                            };
                            return await Task.FromResult(customResponse);
                        }
                        return await Task.FromResult(response.Result);
                    },
                    async (response, context) =>
                    {
                        await Task.CompletedTask;
                    });

            return fallbackPolicy.WrapAsync(retryPolicy);
        }

        public static Policy PollyRetryForSFTPUpload(Action<string, string> logFunction, int numberOfRetry = CommonConstants.NUMBER_OF_RETRIES)
        {
            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetry(numberOfRetry,
                retryAttempt => CalculateRetryDelay(retryAttempt),
                (response, waitTime, retryCount, context) =>
                {
                    logFunction(CommonConstants.INFO, string.Format(LogCommonStrings.RETRY_LOG_FORMAT, retryCount, numberOfRetry, waitTime.TotalSeconds));
                });

            return retryPolicy;
        }
    }
}
