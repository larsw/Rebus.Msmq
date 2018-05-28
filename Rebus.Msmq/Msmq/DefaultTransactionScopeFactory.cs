using System;
using System.Transactions;

namespace Rebus.Msmq
{
    /// <summary>
    /// 
    /// </summary>
    public class DefaultTransactionScopeFactory : ITransactionScopeFactory
    {
        private readonly TransactionOptions _transactionOptions;

        /// <summary>
        /// 
        /// </summary>
        public DefaultTransactionScopeFactory()
        {
            _transactionOptions = new TransactionOptions
            {
                Timeout = TimeSpan.FromSeconds(59),
                IsolationLevel = IsolationLevel.ReadCommitted
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="transactionOptions"></param>
        public DefaultTransactionScopeFactory(TransactionOptions transactionOptions)
        {
            _transactionOptions = transactionOptions;
        }

        /// <inheritdoc />
        public TransactionScope CreateTransactionScope()
        {
            return new TransactionScope(TransactionScopeOption.RequiresNew, _transactionOptions, TransactionScopeAsyncFlowOption.Enabled);
        }
    }
}