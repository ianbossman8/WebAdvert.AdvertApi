using System;
using System.Threading.Tasks;
using AdvertApi.Models;
using AutoMapper;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using System.Collections.Generic;

namespace AdvertApi.Services
{
    public class DynamoDBAdvertStorage : IAdvertStorageService
    {
        private readonly IMapper _mapper;

        public DynamoDBAdvertStorage(IMapper mapper)
        {
            _mapper = mapper;
        }

        public async Task<string> Add(AdvertModel model)
        {
            AdvertDbModel dbModel = _mapper.Map<AdvertDbModel>(model);

            dbModel.Id = Guid.NewGuid().ToString();
            dbModel.CreationDateTime = DateTime.UtcNow;
            dbModel.Status = AdvertStatus.Pending;

            using (AmazonDynamoDBClient client = new AmazonDynamoDBClient())
            {
                DescribeTableResponse table = await client.DescribeTableAsync("Adverts");

                bool tableStatus = string.Compare(table.Table.TableStatus, "active", true) == 0;

                if (tableStatus)
                {
                    using (DynamoDBContext context = new DynamoDBContext(client))
                    {
                        await context.SaveAsync(dbModel);
                    }

                }

            }

            return dbModel.Id;
        }


        public async Task Confirm(ConfirmAdvertModel model)
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
                        result.FilePath = model.FilePath;
                        result.Status = AdvertStatus.Active;

                        await context.SaveAsync(result);
                    }
                    else
                    {
                        await context.DeleteAsync(result);
                    }
                }
            }
        }

        public async Task<bool> CheckHealthAsync()
        {
            using (AmazonDynamoDBClient client = new AmazonDynamoDBClient())
            {
                DescribeTableResponse tableData = await client.DescribeTableAsync("Adverts");

                return string.Compare(tableData.Table.TableStatus, "active", true) == 0;
            }
        }

        public async Task<string> GetById(string id)
        {
            using (AmazonDynamoDBClient client = new AmazonDynamoDBClient())
            {
                using (DynamoDBContext context = new DynamoDBContext(client))
                {
                    AdvertDbModel dbModel = await context.LoadAsync<AdvertDbModel>(id);

                    if (dbModel == null)
                    {
                        throw new Exception($"key not found {id}");
                    }

                    return dbModel.Title;
                }
            }

            throw new KeyNotFoundException();
        }
    }
}
