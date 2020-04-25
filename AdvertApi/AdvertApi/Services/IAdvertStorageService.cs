using System.Threading.Tasks;
using AdvertApi.Models;

namespace AdvertApi.Services
{
    public interface IAdvertStorageService
    {
        Task<string> Add(AdvertModel model);
        Task<bool> Confirm(ConfirmAdvertModel model);
    }
}
