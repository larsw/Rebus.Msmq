using System;
using System.Collections.Generic;
using System.Messaging;
using Rebus.Msmq;

namespace Rebus.Config
{
    /// <summary>
    /// MSMQ configuration builder that can be used to customize how certain MSMQ operations are performed
    /// </summary>
    public class MsmqTransportConfigurationBuilder
    {
        readonly List<Action<MessageQueue>> _onCreatedCallbacks = new List<Action<MessageQueue>>();

        /// <summary>
        /// Adds a callback to be invoked when a new queue is created. Callbacks will be called AFTER the default permissions
        /// have been applied, which means that it is possible to e.g. remove the Administrators group's <see cref="MessageQueueAccessRights.FullControl"/> 
        /// permission again in order to avoid this particular default
        /// </summary>
        public MsmqTransportConfigurationBuilder OnCreated(Action<MessageQueue> callback)
        {
            _onCreatedCallbacks.Add(callback);
            return this;
        }

        internal virtual void Configure(BaseMsmqTransport transport)
        {
            _onCreatedCallbacks.ForEach(transport.AddQueueCallback);
        }
    }

    public class AmbientTxScopeAwareMsmqTransportConfigurationBuilder : MsmqTransportConfigurationBuilder
    {
        private ITransactionScopeFactory _transactionScopeFactory;

        public AmbientTxScopeAwareMsmqTransportConfigurationBuilder WithTransactionScopeFactory(
            ITransactionScopeFactory transactionScopeFactory)
        {
            _transactionScopeFactory = transactionScopeFactory;
            return this;
        }

        internal override void Configure(BaseMsmqTransport transport)
        {
            base.Configure(transport);
            if (transport is AmbientTxScopeAwareMsmqTransport txAwareTransport)
            {
                var factory = _transactionScopeFactory ?? new DefaultTransactionScopeFactory();
                txAwareTransport.SetTransactionScopeFactory(factory);
            }
        }
    }
}