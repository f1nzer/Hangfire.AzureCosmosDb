using System.Threading.Tasks;

namespace Hangfire.Azure.Helper;

internal static class AsyncHelper
{
	public static void ExecuteSynchronously(this Task task) => task.ConfigureAwait(false).GetAwaiter().GetResult();

    public static T ExecuteSynchronously<T>(this Task<T> task) => task.ConfigureAwait(false).GetAwaiter().GetResult();
}