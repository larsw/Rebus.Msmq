using System.Transactions;

namespace Rebus.Msmq
{
    /// <summary>
    /// 
    /// </summary>
    public interface ITransactionScopeFactory
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        TransactionScope CreateTransactionScope();
    }
}