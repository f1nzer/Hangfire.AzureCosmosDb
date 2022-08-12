using Hangfire;
using Hangfire.AzureCosmosDB.Sample;
using Microsoft.Azure.Cosmos.Fluent;
using LogLevel = Hangfire.Logging.LogLevel;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<ToDoService>();
builder.Services.AddControllers();
builder.Services.AddHangfireServer(x => { x.WorkerCount = 25; });
builder.Services.AddHangfireServer(x =>
{
	x.ServerName = "Server2";
	x.WorkerCount = 25;
});
builder.Services.AddHangfireServer(x =>
{
	x.ServerName = "Server3";
	x.WorkerCount = 25;
});

// use cosmos emulator or free cosmos plan from azure
const string url = "https://localhost:8081";
const string secretKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
const string database = "hangfire";
const string collection = "hangfire-test";

builder.Services.AddHangfire(o =>
{
	// o.UseAzureCosmosDbStorage(url, secretKey, database, collection);
    o.UseAzureCosmosDbStorage(new CosmosClientBuilder(url, secretKey), database, collection);
	o.UseColouredConsoleLogProvider(LogLevel.Trace);
});

WebApplication app = builder.Build();
app.UseStaticFiles();
app.UseHangfireDashboard();
app.UseRouting();
app.UseEndpoints(x => { x.MapControllers(); });

using (IServiceScope scope = app.Services.CreateScope())
{
	ToDoService service = scope.ServiceProvider.GetRequiredService<ToDoService>();
	RecurringJob.AddOrUpdate("TO_DO_TASK_JOB", () => service.DoTask(), Cron.Minutely());
	RecurringJob.AddOrUpdate("TO_DO_ANOTHER_TASK_JOB", () => service.DoAnotherTask(), Cron.Hourly(15), TimeZoneInfo.Local);
}

app.Run();