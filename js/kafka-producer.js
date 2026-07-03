module.exports = function (RED) {
    const kafka = require('kafka-node');

    function KafkaProducerNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // ---- runtime state ----
        node._ready = false;
        node._client = null;
        node._producer = null;
        node._onReady = null;
        node._onError = null;
        node._interval = null;
        node._lastMessageTs = null;
        node._activeCfg = null;     // effective runtime config used to build client/producer
        node._knownTopics = new Set();
        node._pending = [];         // queued send closures until ready

        // ---- helpers ----
        function coerceNumber(n, defVal) {
            const v = Number(n);
            return Number.isFinite(v) ? v : defVal;
        }
        function shallowEqual(a, b) {
            try { return JSON.stringify(a || null) === JSON.stringify(b || null); } catch { return false; }
        }
        function safeStatus(obj) { try { node.status(obj || {}); } catch (_) { } }

        function connShape(eff) {
            return {
                kafkaClientOptions: eff && eff.kafkaClientOptions ? eff.kafkaClientOptions : {},
                producerOptions: eff && eff.producerOptions ? eff.producerOptions : {}
            };
        }

        // Accept common aliases: kafkaHost, bootstrapServers, bootstrap.servers, brokers, servers, hosts, and array forms
        function normalizeKafkaHost(mcfg, brokerOptions) {
            let host = null;
            const pick = (v) => {
                if (!v) return null;
                if (typeof v === 'string') return v.trim() || null;
                if (Array.isArray(v)) return v.filter(Boolean).join(',') || null;
                if (typeof v === 'object') {
                    if (Array.isArray(v.list)) return v.list.filter(Boolean).join(',') || null;
                    if (Array.isArray(v.brokers)) return v.brokers.filter(Boolean).join(',') || null;
                }
                return null;
            };
            host = pick(mcfg.kafkaHost)
                || pick(mcfg.bootstrapServers)
                || pick(mcfg['bootstrap.servers'])
                || pick(mcfg.brokers)
                || pick(mcfg.servers)
                || pick(mcfg.hosts)
                || pick(brokerOptions && (brokerOptions.kafkaHost || brokerOptions.bootstrapServers || brokerOptions['bootstrap.servers']));
            return host;
        }

        // Build effective config from msg first; editor config is fallback only (but never required)
        function resolveCfg(msg) {
            const mcfg = (msg && msg.kafkaCfg) || {};

            // Optional editor fallback (do NOT require broker node; node must be savable without it)
            let brokerOptions = null;
            if (config.broker) {
                const brokerCfgNode = RED.nodes.getNode(config.broker);
                if (brokerCfgNode && typeof brokerCfgNode.getOptions === 'function') {
                    try { brokerOptions = brokerCfgNode.getOptions(); } catch (_) { }
                }
            }

            const clientOpts = Object.assign({}, brokerOptions, mcfg.kafkaClientOptions || {});
            const normalizedHost = normalizeKafkaHost(mcfg, brokerOptions);
            if (normalizedHost) clientOpts.kafkaHost = normalizedHost;
            Object.keys(clientOpts).forEach(k => (clientOpts[k] === undefined) && delete clientOpts[k]);

            const effective = {
                kafkaClientOptions: clientOpts,
                producerOptions: {
                    requireAcks: coerceNumber(mcfg.requireAcks ?? config.requireAcks, 1),
                    ackTimeoutMs: coerceNumber(mcfg.ackTimeoutMs ?? config.ackTimeoutMs, 100),
                },
                topic: mcfg.topic ?? config.topic,
                attributes: coerceNumber(mcfg.attributes ?? config.attributes, 0),
            };
            const msgTopic = (msg && (msg.topic != null ? msg.topic : (msg.payload && msg.payload.topic)));
            if (msgTopic != "" && msgTopic != null) {
                try { effective.topic = msgTopic; } catch (_) { }
            }
            return effective;
        }

        function destroyProducer() {
            node._ready = false;
            if (node._interval) { clearInterval(node._interval); node._interval = null; }
            if (node._producer) {
                try {
                    node._producer.removeListener('ready', node._onReady);
                    node._producer.removeListener('error', node._onError);
                } catch (_) { }
                try { node._producer.close(() => { }); } catch (_) { }
                node._producer = null;
            }
            if (node._client) {
                try { node._client.close(() => { }); } catch (_) { }
                node._client = null;
            }
        }

        // --- metadata helpers ---
        function refreshTopicMetadata(topic) {
            return new Promise((resolve) => {
                try { node._client.refreshMetadata([topic], () => resolve()); }
                catch (_) { resolve(); }
            });
        }
        function sendWithMetaRefresh(topic, payload) {
            return new Promise(async (resolve, reject) => {
                const attempt = (refreshed) => {
                    try {
                        node._producer.send([payload], (err, data) => {
                            if (!err) return resolve(data);
                            const msg = (err && err.message) || '';
                            if (!refreshed && /(topic\/partition change check failed|Broker not available|LeaderNotAvailable|UnknownTopicOrPartition)/i.test(msg)) {
                                return refreshTopicMetadata(topic).then(() => attempt(true));
                            }
                            return reject(err);
                        });
                    } catch (e) { return reject(e); }
                };
                if (!node._knownTopics.has(topic)) { await refreshTopicMetadata(topic); node._knownTopics.add(topic); }
                attempt(false);
            });
        }

        function doSend(topic, attributes, messagePayload, msg, send, done) {
            const sendOptions = { topic, attributes, messages: [messagePayload] };
            safeStatus({ fill: 'blue', shape: 'dot', text: 'Sending' });
            sendWithMetaRefresh(topic, sendOptions)
                .then((data) => {
                    node._lastMessageTs = Date.now();
                    safeStatus({ fill: 'blue', shape: 'dot', text: 'Sent' });
                    try { msg.payload = data; (send || node.send)(msg); } catch (_) { }
                    done && done();
                })
                .catch((err) => {
                    node._lastMessageTs = null;
                    safeStatus({ fill: 'red', shape: 'ring', text: 'Send error' });
                    node.warn(err);
                    done && done(err);
                });
        }

        function drainPending() {
            
            if (!node._ready || !node._producer) return;
            const queue = node._pending.splice(0);

            for (const fn of queue) {
                try { fn(); } catch (e) { node.warn(e); }
            }
        }

        function ensureProducer(effectiveCfg) {
            // Rebuild if not ready or connection config changed (ignore topic/attributes)
            const nextConn = connShape(effectiveCfg);
            if (!node._ready || !shallowEqual(node._activeCfg, nextConn)) {
                destroyProducer();
                node._activeCfg = nextConn;

                const opts = effectiveCfg.kafkaClientOptions || {};
                if (!opts.kafkaHost && !opts.host && !opts.connectionString) {
                    // Do not throw; allow node to be saved and flow to deploy; wait for msg.kafkaCfg
                    safeStatus({ fill: 'yellow', shape: 'ring', text: 'waiting for kafkaCfg.kafkaHost' });
                    return false;
                }

                node._client = new kafka.KafkaClient(opts);
                node._producer = new kafka.HighLevelProducer(node._client, effectiveCfg.producerOptions || {});
                safeStatus({ fill: 'blue', shape: 'ring', text: 'connecting...' });

                node._onError = function (err) {
                    node._ready = false;
                    node._lastMessageTs = null;
                    safeStatus({ fill: 'red', shape: 'ring', text: (err && err.message) ? err.message : 'Error' });
                    node.error(err, msg);
                };
                node._onReady = function () {
                    node._ready = true;
                    node._lastMessageTs = Date.now();
                    safeStatus({ fill: 'green', shape: 'dot', text: 'Ready' });
                    // Prime known topic from config if present
                    if (effectiveCfg && effectiveCfg.topic) { try { node._knownTopics.add(effectiveCfg.topic); } catch (_) { } }
                    drainPending();
                };

                node._producer.on('ready', node._onReady);
                node._producer.on('error', node._onError);

                node._interval = setInterval(() => {
                    if (node._lastMessageTs != null) {
                        const diff = Date.now() - node._lastMessageTs;
                        if (diff > 5000) safeStatus({ fill: 'yellow', shape: 'ring', text: 'Idle' });
                    }
                }, 1000);
            }
            return node._ready;
        }

        node.on('input', function (msg, send, done) {
            try {
                const eff = resolveCfg(msg);
                // Resolve per-message topic and payload now (avoid future mutations)
                const topic =  eff.topic;
                if (!topic) { safeStatus({ fill: 'yellow', shape: 'ring', text: 'no topic' }); done && done(); return; }
                const attributes = (msg.attributes != null ? msg.attributes : eff.attributes);

                let messagePayload = (msg.payload && msg.payload.body != null) ? msg.payload.body : msg.payload;
                if (messagePayload == null) messagePayload = '';
                else if (!Buffer.isBuffer(messagePayload) && typeof messagePayload !== 'string') {
                    try { messagePayload = JSON.stringify(messagePayload); } catch { messagePayload = String(messagePayload); }
                }
                const readyNow = ensureProducer(eff);
                if (!readyNow) {
                    node._pending.push(() => doSend(topic, attributes, messagePayload, msg, send, done));
                    // 兜底：无论当前是否 ready，都安排一次异步 drain；ready 到来后会真正执行
                    setTimeout(drainPending, 0);      // 或者：Promise.resolve().then(drainPending)
                    return;
                }
                doSend(topic, attributes, messagePayload, msg, send, done);
            } catch (e) {
                safeStatus({ fill: 'red', shape: 'ring', text: 'Unhandled error' });
                node.error(e, msg);
                done && done(e);
            }
        });

        node.on('close', function () {
            destroyProducer();
            safeStatus({});
        });
    }

    RED.nodes.registerType('zkafka-producer', KafkaProducerNode);
}