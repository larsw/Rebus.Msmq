﻿using Rebus.Logging;
using Rebus.Msmq;
using Rebus.Transport;

namespace Rebus.Config
{
    /// <summary>
    /// Configuration extensions for the MSMQ transport
    /// </summary>
    public static class MsmqTransportConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use MSMQ to transport messages, receiving messages from the specified <paramref name="inputQueueName"/>
        /// </summary>
        public static MsmqTransportConfigurationBuilder UseMsmq(this StandardConfigurer<ITransport> configurer, string inputQueueName)
        {
            var builder = new MsmqTransportConfigurationBuilder();

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var transport = new MsmqTransport(inputQueueName, rebusLoggerFactory);
                builder.Configure(transport);
                return transport;
            });

            return builder;
        }

        /// <summary>
        /// Configures Rebus to use MSMQ to transport messages, receiving messages from the specified <paramref name="inputQueueName"/>
        /// </summary>
        public static AmbientTxScopeAwareMsmqTransportConfigurationBuilder UseMsmqWithAmbientTransactionSupport(this StandardConfigurer<ITransport> configurer, string inputQueueName)
        {
            var builder = new AmbientTxScopeAwareMsmqTransportConfigurationBuilder();

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var transport = new AmbientTxScopeAwareMsmqTransport(inputQueueName, rebusLoggerFactory);
                builder.Configure(transport);
                return transport;
            });

            return builder;
        }

        /// <summary>
        /// Configures Rebus to use MSMQ to transport messages as a one-way client (i.e. will not be able to receive any messages)
        /// </summary>
        public static MsmqTransportConfigurationBuilder UseMsmqAsOneWayClient(this StandardConfigurer<ITransport> configurer)
        {
            var builder = new MsmqTransportConfigurationBuilder();

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var transport = new MsmqTransport(null, rebusLoggerFactory);
                builder.Configure(transport);
                return transport;
            });

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);

            return builder;
        }

        /// <summary>
        /// Configures Rebus to use MSMQ to transport messages as a one-way client (i.e. will not be able to receive any messages)
        /// </summary>
        public static MsmqTransportConfigurationBuilder UseMsmqWithAmbientTransactionSupportAsAOneWayClient(this StandardConfigurer<ITransport> configurer)
        {
            var builder = new AmbientTxScopeAwareMsmqTransportConfigurationBuilder();

            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var transport = new AmbientTxScopeAwareMsmqTransport(null, rebusLoggerFactory);
                builder.Configure(transport);
                return transport;
            });

            OneWayClientBackdoor.ConfigureOneWayClient(configurer);

            return builder;
        }
    }
}