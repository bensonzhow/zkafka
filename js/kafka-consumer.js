module.exports = function(RED) {
    const kafka = require('kafka-node');

    function KafkaConsumerNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // ---- runtime state ----
        node._group = null;       // kafka ConsumerGroup instance
        node._activeCfg = null;   // last effective cfg used to build consumer
        node._ready = false;
        node._lastMessageTs = null;
        node._interval = null;

        // ---- helpers ----
        function uuid4() {
            let u = '', i = 0;
            while (i++ < 36) {
                const c = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'[i - 1];
                const r = Math.random() * 16 | 0;
                const v = c === 'x' ? r : (r & 0x3 | 0x8);
                u += (c === '-' || c === '4') ? c : v.toString(16);
            }
            return u;
        }
        function safeStatus(obj){ try { node.status(obj || {}); } catch(_){} }
        function coerceNumber(n, defVal){ const v = Number(n); return Number.isFinite(v) ? v : defVal; }
        function shallowEqual(a,b){ try { return JSON.stringify(a||null) === JSON.stringify(b||null); } catch { return false; } }

        // Build effective config (msg first)
        function resolveCfg(msg){
            const mcfg = (msg && msg.kafkaCfg) || {};

            // editor broker fallback (optional)
            let brokerOptions = null;
            if (config.broker) {
                const brokerNode = RED.nodes.getNode(config.broker);
                if (brokerNode && typeof brokerNode.getOptions === 'function') {
                    try { brokerOptions = brokerNode.getOptions(); } catch(_){}
                }
            }

            // Merge order: brokerOptions <- mcfg.kafkaClientOptions, then explicit mcfg.kafkaHost overrides
            const clientOpts = Object.assign({}, brokerOptions, mcfg.kafkaClientOptions || {});
            if (mcfg.kafkaHost) clientOpts.kafkaHost = mcfg.kafkaHost; // highest priority

            const topic = (msg && (msg.topic ?? (msg.payload && msg.payload.topic))) ?? mcfg.topic ?? config.topic;
            const groupId = mcfg.groupId ?? config.groupid ?? (`nodered_kafka_client_${uuid4()}`);

            const effective = {
                clientOpts,
                groupOpts: {
                    kafkaHost: clientOpts.kafkaHost,  // kafka-node reads from group options as well
                    groupId,
                    fromOffset: mcfg.fromOffset ?? config.fromOffset ?? 'latest',
                    outOfRangeOffset: mcfg.outOfRangeOffset ?? config.outOfRangeOffset ?? 'earliest',
                    fetchMinBytes: coerceNumber(mcfg.minbytes ?? config.minbytes, 1),
                    fetchMaxBytes: coerceNumber(mcfg.maxbytes ?? config.maxbytes, 1048576),
                    encoding: mcfg.encoding ?? config.encoding ?? 'utf8',
                    // pass-through extra options if provided
                    sessionTimeout: coerceNumber(mcfg.sessionTimeout, undefined),
                    protocol: mcfg.protocol,
                    id: mcfg.clientId
                },
                topic
            };
            // prune undefineds to keep stable JSON for shallowEqual
            Object.keys(effective.groupOpts).forEach(k => (effective.groupOpts[k] === undefined) && delete effective.groupOpts[k]);
            return effective;
        }

        function destroyConsumer(){
            node._ready = false;
            if (node._interval){ clearInterval(node._interval); node._interval = null; }
            if (node._group){
                try {
                    node._group.removeAllListeners('connect');
                    node._group.removeAllListeners('message');
                    node._group.removeAllListeners('error');
                    node._group.removeAllListeners('offsetOutOfRange');
                } catch(_){}
                try { node._group.close(true, ()=>{}); } catch(_){}
                node._group = null;
            }
        }

        function ensureConsumer(eff){
            // If not ready or config changed -> rebuild
            if (!node._ready || !shallowEqual(node._activeCfg, eff)){
                destroyConsumer();
                node._activeCfg = eff;

                // no broker info yet? do not throw; wait for next msg
                const hasBroker = eff.clientOpts && (eff.clientOpts.kafkaHost || eff.clientOpts.host || eff.clientOpts.connectionString);
                if (!hasBroker){
                    safeStatus({ fill:'yellow', shape:'ring', text:'waiting for kafkaCfg.kafkaHost' });
                    return false;
                }
                // no topic yet? also just wait, do not error
                if (!eff.topic){
                    safeStatus({ fill:'yellow', shape:'ring', text:'waiting for topic' });
                    return false;
                }

                try {
                    node._group = new kafka.ConsumerGroup(Object.assign({}, eff.groupOpts), eff.topic);
                } catch (e){
                    safeStatus({ fill:'red', shape:'ring', text:'init failed' });
                    node.warn(e);
                    return false;
                }

                node._group.on('connect', () => {
                    node._ready = true;
                    node._lastMessageTs = Date.now();
                    safeStatus({ fill:'green', shape:'dot', text:'Ready' });
                });
                node._group.on('message', (message) => {
                    node._lastMessageTs = Date.now();
                    node.status({ fill:'blue', shape:'dot', text:'Reading' });
                    node.send({ payload: message });
                });
                node._group.on('error', (err) => {
                    node._ready = false;
                    node._lastMessageTs = null;
                    safeStatus({ fill:'red', shape:'ring', text: (err && err.message) ? err.message : 'Error' });
                    node.warn(err);
                });
                node._group.on('offsetOutOfRange', (err) => {
                    safeStatus({ fill:'yellow', shape:'ring', text:'offsetOutOfRange' });
                    node.warn(err);
                });

                node._interval = setInterval(() => {
                    if (node._lastMessageTs != null){
                        const diff = Date.now() - node._lastMessageTs;
                        if (diff > 5000){
                            safeStatus({ fill:'yellow', shape:'ring', text:'Idle' });
                        }
                    }
                }, 1000);
            }
            return node._ready;
        }

        node.on('input', function(msg, send, done){
            try {
                const eff = resolveCfg(msg);
                ensureConsumer(eff);
                done && done();
            } catch (e){
                safeStatus({ fill:'red', shape:'ring', text:'Unhandled error' });
                node.error(e, msg);
                done && done(e);
            }
        });

        node.on('close', function(){
            destroyConsumer();
            safeStatus({});
        });

        // do not auto-init here; wait for first msg so editor can remain empty without errors
    }

    RED.nodes.registerType('zkafka-consumer', KafkaConsumerNode);
}
