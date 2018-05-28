using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Transport;
using Message = System.Messaging.Message;

namespace Rebus.Msmq
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class BaseMsmqTransport : ITransport, IDisposable
    {
        /// <summary>
        /// 
        /// </summary>
        public const string CurrentTransactionKey = "msmqtransport-messagequeuetransaction";

        /// <summary>
        /// 
        /// </summary>
        public const string CurrentOutgoingQueuesKey = "msmqtransport-outgoing-messagequeues";

        private readonly List<Action<MessageQueue>> _newQueueCallbacks = new List<Action<MessageQueue>>();
        private readonly ExtensionSerializer _extensionSerializer = new ExtensionSerializer();
        private volatile MessageQueue _inputQueue;
        private bool _disposed;

        /// <summary>
        /// 
        /// </summary>
        protected string InputQueueName { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        protected ILog Log { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="inputQueueAddress"></param>
        /// <param name="rebusLoggerFactory"></param>
        protected BaseMsmqTransport(string inputQueueAddress, IRebusLoggerFactory rebusLoggerFactory)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));

            Log = rebusLoggerFactory.GetLogger<MsmqTransport>();

            if (inputQueueAddress != null)
            {
                InputQueueName = MakeGloballyAddressable(inputQueueAddress);
            }
        }

        /// <inheritdoc />
        public abstract Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken);

        /// <inheritdoc />
        public abstract Task Send(string destinationAddress, TransportMessage message, ITransactionContext context);

        /// <summary>
        /// Gets the input queue address of this MSMQ queue
        /// </summary>
        public string Address => InputQueueName;

        /// <summary>
        /// Adds a callback to be invoked when a new queue is created. Can be used e.g. to customize permissions
        /// </summary>
        public void AddQueueCallback(Action<MessageQueue> callback)
        {
            _newQueueCallbacks.Add(callback);
        }

        /// <summary>
        /// Initializes the transport by creating the input queue
        /// </summary>
        public void Initialize()
        {
            if (InputQueueName != null)
            {
                Log.Info("Initializing MSMQ transport - input queue: '{0}'", InputQueueName);

                GetInputQueue();
            }
            else
            {
                Log.Info("Initializing one-way MSMQ transport");
            }
        }

        /// <summary>
        /// Creates a queue with the given address, unless the address is of a remote queue - in that case,
        /// this call is ignored
        /// </summary>
        public void CreateQueue(string address)
        {
            if (!MsmqUtil.IsLocal(address)) return;

            var inputQueuePath = MsmqUtil.GetPath(address);

            if (_newQueueCallbacks.Any())
            {
                MsmqUtil.EnsureQueueExists(inputQueuePath, Log, messageQueue =>
                {
                    _newQueueCallbacks.ForEach(callback => callback(messageQueue));
                });
            }
            else
            {
                MsmqUtil.EnsureQueueExists(inputQueuePath, Log);
            }

            MsmqUtil.EnsureMessageQueueIsTransactional(inputQueuePath);
        }


        /// <summary>
        /// Deletes all messages in the input queue
        /// </summary>
        public void PurgeInputQueue()
        {
            if (!MsmqUtil.QueueExists(InputQueueName))
            {
                Log.Info("Purging {0} (but the queue doesn't exist...)", InputQueueName);
                return;
            }

            Log.Info("Purging {0}", InputQueueName);
            MsmqUtil.PurgeQueue(InputQueueName);
        }

        /// <summary>
        /// Disposes the input message queue object
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _inputQueue?.Dispose();
                _inputQueue = null;
            }
            finally
            {
                _disposed = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        protected Message CreateMsmqMessage(TransportMessage message)
        {
            var headers = message.Headers;

            var expressDelivery = headers.ContainsKey(Headers.Express);

            var hasTimeout = headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedStr);

            var msmqMessage = new Message
            {
                Extension = _extensionSerializer.Serialize(headers),
                BodyStream = new MemoryStream(message.Body),
                UseJournalQueue = false,
                Recoverable = !expressDelivery,
                UseDeadLetterQueue = !(expressDelivery || hasTimeout),
                Label = GetMessageLabel(message),
            };

            if (hasTimeout)
            {
                msmqMessage.TimeToBeReceived = TimeSpan.Parse(timeToBeReceivedStr);
            }

            return msmqMessage;
        }

        /// <summary>
        /// 
        /// </summary>
        protected void ReinitializeInputQueue()
        {
            if (_inputQueue != null)
            {
                try
                {
                    _inputQueue.Close();
                    _inputQueue.Dispose();
                }
                catch (Exception exception)
                {
                    Log.Warn("An error occurred when closing/disposing the queue handle for '{0}': {1}", InputQueueName, exception);
                }
                finally
                {
                    _inputQueue = null;
                }
            }

            GetInputQueue();

            Log.Info("Input queue handle successfully reinitialized");
        }

        protected MessageQueue GetInputQueue()
        {
            if (_inputQueue != null) return _inputQueue;

            lock (this)
            {
                if (_inputQueue != null) return _inputQueue;

                var inputQueuePath = MsmqUtil.GetPath(InputQueueName);

                if (_newQueueCallbacks.Any())
                {
                    MsmqUtil.EnsureQueueExists(inputQueuePath, Log, messageQueue =>
                    {
                        _newQueueCallbacks.ForEach(callback => callback(messageQueue));
                    });
                }
                else
                {
                    MsmqUtil.EnsureQueueExists(inputQueuePath, Log);
                }
                MsmqUtil.EnsureMessageQueueIsTransactional(inputQueuePath);

                _inputQueue = new MessageQueue(inputQueuePath, QueueAccessMode.SendAndReceive)
                {
                    MessageReadPropertyFilter = new MessagePropertyFilter
                    {
                        Id = true,
                        Extension = true,
                        Body = true,
                    }
                };
            }

            return _inputQueue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        protected async Task<TransportMessage> ConstructTransportMessage(CancellationToken cancellationToken, Message message)
        {
            var headers = _extensionSerializer.Deserialize(message.Extension);
            var body = new byte[message.BodyStream.Length];

            await message.BodyStream.ReadAsync(body, 0, body.Length, cancellationToken);

            return new TransportMessage(headers, body);
        }

        private static string GetMessageLabel(TransportMessage message)
        {
            try
            {
                return message.GetMessageLabel();
            }
            catch
            {
                // if that failed, it's most likely because we're running in legacy mode - therefore:
                return message.Headers.GetValueOrNull(Headers.MessageId)
                       ?? message.Headers.GetValueOrNull("rebus-msg-id")
                       ?? "<unknown ID>";
            }
        }

        private static string MakeGloballyAddressable(string inputQueueName)
        {
            return inputQueueName.Contains("@")
                ? inputQueueName
                : $"{inputQueueName}@{Environment.MachineName}";
        }

        /// <summary>
        /// 
        /// </summary>
        protected class ExtensionSerializer
        {
            readonly HeaderSerializer _utf8HeaderSerializer = new HeaderSerializer { Encoding = Encoding.UTF8 };

            /// <summary>
            /// 
            /// </summary>
            /// <param name="headers"></param>
            /// <returns></returns>
            public byte[] Serialize(Dictionary<string, string> headers)
            {
                return _utf8HeaderSerializer.Serialize(headers);
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="bytes"></param>
            /// <returns></returns>
            public Dictionary<string, string> Deserialize(byte[] bytes)
            {
                return _utf8HeaderSerializer.Deserialize(bytes);
            }

            /// <summary>
            /// TODO Can be deleted?
            /// </summary>
            static bool IsUtf7(byte[] bytes)
            {
                // auto-detect UTF7-encoded headers
                // 43, 65, 72, 115 == an UTF7-encoded '{'
                if (bytes.Length < 4) return false;

                return bytes[0] == 43 && bytes[1] == 65 && bytes[2] == 72 && bytes[3] == 115;
            }
        }
    }
}