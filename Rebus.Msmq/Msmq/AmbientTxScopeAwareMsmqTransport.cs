using System;
using System.Collections.Concurrent;
using System.IO;
using System.Messaging;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Transport;

#pragma warning disable 1998

namespace Rebus.Msmq
{
    using System.Transactions;

    /// <summary>
    /// Implementation of <see cref="ITransport"/> that uses MSMQ to do its thing in an ambient transaction, if available.
    /// </summary>
    public class AmbientTxScopeAwareMsmqTransport : BaseMsmqTransport
    {
        private ITransactionScopeFactory _transactionScopeFactory = new DefaultTransactionScopeFactory();

        /// <summary>
        /// Constructs the transport with the specified input queue address, and optionally a <see cref="TransactionScope"/> factory.
        /// </summary>
        public AmbientTxScopeAwareMsmqTransport(string inputQueueAddress, IRebusLoggerFactory rebusLoggerFactory)
            :base(inputQueueAddress, rebusLoggerFactory)
        {
        }

        /// <summary>
        /// Sends the given transport message to the specified destination address using MSMQ. Will use the existing <see cref="MessageQueueTransaction"/> stashed
        /// under the <see cref="BaseMsmqTransport.CurrentTransactionKey"/> key in the given <paramref name="context"/>, or else it will create one and add it.
        /// </summary>
        public override async Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
        {
            if (destinationAddress == null) throw new ArgumentNullException(nameof(destinationAddress));
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var logicalMessage = CreateMsmqMessage(message);

            // Establish an ambient transaction if one is not present on the current thread.
            if (Transaction.Current != null)
            {
                Transaction.Current = context.GetOrAdd(CurrentTransactionKey, () =>
                {
                    var txScope = _transactionScopeFactory.CreateTransactionScope();
                    context.OnCommitted(async () =>
                    {
                        txScope.Complete();
                    });
                    context.OnDisposed(() =>
                    {
                        txScope.Dispose();
                    });

                    return Transaction.Current;
                });
            }

            var sendQueues = context.GetOrAdd(CurrentOutgoingQueuesKey, () =>
            {
                var messageQueues = new ConcurrentDictionary<string, MessageQueue>(StringComparer.InvariantCultureIgnoreCase);

                context.OnDisposed(() =>
                {
                    foreach (var messageQueue in messageQueues.Values)
                    {
                        messageQueue.Dispose();
                    }
                });

                return messageQueues;
            });

            var path = MsmqUtil.GetFullPath(destinationAddress);

            var sendQueue = sendQueues.GetOrAdd(path, _ =>
            {
                var messageQueue = new MessageQueue(path, QueueAccessMode.Send);

                return messageQueue;
            });

            try
            {
                sendQueue.Send(logicalMessage, MessageQueueTransactionType.Automatic);
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not send to MSMQ queue with path '{sendQueue.Path}'");
            }
        }

        /// <summary>
        /// Received the next available transport message from the input queue via MSMQ. Will create a new <see cref="MessageQueueTransaction"/> and stash
        /// it under the <see cref="BaseMsmqTransport.CurrentTransactionKey"/> key in the given <paramref name="context"/>. If one already exists, an exception will be thrown
        /// (because we should never have to receive multiple messages in the same transaction)
        /// </summary>
        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (InputQueueName == null)
            {
                throw new InvalidOperationException("This MSMQ transport does not have an input queue, hence it is not possible to reveive anything");
            }

            var queue = GetInputQueue();

            if (context.Items.ContainsKey(CurrentTransactionKey))
            {
                throw new InvalidOperationException("Tried to receive with an already existing MSMQ queue transaction - although it is possible with MSMQ to do so, with Rebus it is an indication that something is wrong!");
            }

            var receiveTimeout = TimeSpan.FromSeconds(59);
            TransactionScope txScope = null;
            try
            {
                // Wait/block for a message to arrive on the queue.
                //queue.Peek();
                txScope = _transactionScopeFactory.CreateTransactionScope();
                context.OnDisposed(() => txScope.Dispose());
                context.Items[CurrentTransactionKey] = Transaction.Current;

                // Note the short timeout; a message will be present on the queue with high probability.
                var message = queue.Receive(TimeSpan.FromSeconds(0.5), MessageQueueTransactionType.Automatic);

                // If someone else (e.g. worker thread or other process) stole the message, return.
                if (message == null)
                {
                    return null;
                }

                context.OnCompleted(async () => txScope.Complete());
                context.OnDisposed(() => message.Dispose());

                return await ConstructTransportMessage(cancellationToken, message);
            }
            catch (MessageQueueException exception)
            {
                if (exception.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                {
                    txScope.Dispose();
                    Log.Warn($"Receiving the message timed out. The current timeout is set to {receiveTimeout:g} second(s).");
                    return null;
                }

                if (exception.MessageQueueErrorCode == MessageQueueErrorCode.InvalidHandle)
                {
                    Log.Warn($"Queue handle for '{InputQueueName}' was invalid - will try to reinitialize the queue");
                    ReinitializeInputQueue();
                    return null;
                }

                if (exception.MessageQueueErrorCode == MessageQueueErrorCode.QueueDeleted)
                {
                    Log.Warn($"Queue '{InputQueueName}' was deleted - will not receive any more messages");
                    return null;
                }

                throw new IOException($"Could not receive next message from MSMQ queue '{InputQueueName}'", exception);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="factory"></param>
        public void SetTransactionScopeFactory(ITransactionScopeFactory factory)
        {
            _transactionScopeFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        }
    }
}