using System;
using System.Threading.Tasks;
using AdvertApi.Models;
using AutoMapper;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;

namespace AdvertApi.Services
{
    public class DynamoDBAdvertStorage: IAdvertStorageService
    {
        private readonly IMapper _mapper;

        public DynamoDBAdvertStorage(IMapper mapper)
        {
            _mapper = mapper;
        }

        public async Task<string> Add(AdvertModel model)
        {
            AdvertDbModel dbModel = _mapper.Map<AdvertDbModel>(model);
            dbModel.Id = new Guid().ToString();
            dbModel.CreationDateTime = DateTime.UtcNow;
            dbModel.Status = AdvertStatus.Pending;

            using (AmazonDynamoDBClient client = new AmazonDynamoDBClient())
            {
                using (DynamoDBContext context = new DynamoDBContext(client))
                {
                    await context.SaveAsync(dbModel);
                }
            }

            return dbModel.Id;
        }

        public async Task<bool> CheckHealthAsync()
        {
            using (AmazonDynamoDBClient client = new AmazonDynamoDBClient())
            {
                DescribeTableResponse tableData = await client.DescribeTableAsync("Adverts");

                return string.Compare(tableData.Table.TableStatus, "active", true) == 0;
            }
        }

        public async Task<bool> Confirm(ConfirmAdvertModel model)
        {
            using (AmazonDynamoDBClient client = new AmazonDynamoDBClient())
            {
                using (DynamoDBContext context = new DynamoDBContext(client))
                {
                    AdvertDbModel result = await context.LoadAsync<AdvertDbModel>(model.Id);

                    if (result == null)
                    {
                        throw new Exception($"key not found {model.Id}");
                    }
                    if (model.Status == AdvertStatus.Active)
                    {
                        result.Status = AdvertStatus.Active;
                        await context.SaveAsync(result);

                        return true;
                    }
                    else
                    {
                        await context.DeleteAsync(result);
                        return false;
                    }
                }
            }
        }
    }
}
