using Amazon.Lambda.Core;

namespace Amop.Core.Repositories.Environment
{
    public class EnvironmentRepository : IEnvironmentRepository
    {
        public string GetEnvironmentVariable(ILambdaContext context, string key)
        {
            string value = null;
            if (System.Environment.GetEnvironmentVariables().Contains(key))
            {
                value = System.Environment.GetEnvironmentVariable(key);
            }

            if (string.IsNullOrWhiteSpace(value) &&
                context.ClientContext != null &&
                context.ClientContext.Environment != null &&
                context.ClientContext.Environment.ContainsKey(key))
            {
                context.ClientContext.Environment.TryGetValue(key, out value);
            }

            return value;
        }
    }
}
