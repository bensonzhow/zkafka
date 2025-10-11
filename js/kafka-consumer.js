"use strict";

const ConsumerGroup = require('kafka-node').ConsumerGroup;
const kafka = require('kafka-node');

module.exports = function(RED) {
    function KafkaConsumerNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        node._interval = null;
        node._gen = 0; // increases each (re)build; handlers check this to avoid stale emits

        // Configured kafka broker node
        node.broker = RED.nodes.getNode(config.broker);
        node.name = config.name;
        node.topic = config.topic;
        node.groupid = config.groupid;
        node.fromOffset = config.fromOffset || "latest";
        node.outOfRangeOffset = config.outOfRangeOffset || "earliest";
        node.minbytes = config.minbytes || 1;
        node.maxbytes = config.maxbytes || 1024 * 1024;
        node.encoding = config.encoding || "utf8";

        // Runtime state
        node._ready = false;
        node._lastMessageTs = null;
        node._activeCfg = null;
        node._group = null;

        // Helpers
        function shallowEqual(a, b) {
            if (a === b) return true;
            if (!a || !b) return false;
            const aKeys = Object.keys(a);
            const bKeys = Object.keys(b);
            if (aKeys.length !== bKeys.length) return false;
            for (let k of aKeys) {
                if (a[k] !== b[k]) return false;
            }
            return true;
        }

        function normalizeKafkaHost(mcfg, brokerOptions){
            // Accept common aliases and array/object forms
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
            return pick(mcfg.kafkaHost)
                || pick(mcfg.bootstrapServers)
                || pick(mcfg['bootstrap.servers'])
                || pick(mcfg.brokers)
                || pick(mcfg.servers)
                || pick(mcfg.hosts)
                || pick(brokerOptions && (brokerOptions.kafkaHost || brokerOptions.bootstrapServers || brokerOptions['bootstrap.servers']));
        }
        function stableShape(eff){
            // Keep only connection-critical fields + topic to decide rebuild
            const g = eff.groupOpts || {};
            const c = eff.clientOpts || {};
            const shape = {
                kafkaHost: c.kafkaHost,
                groupId: g.groupId,
                fromOffset: g.fromOffset,
                outOfRangeOffset: g.outOfRangeOffset,
                fetchMinBytes: g.fetchMinBytes,
                fetchMaxBytes: g.fetchMaxBytes,
                encoding: g.encoding,
                sessionTimeout: g.sessionTimeout,
                protocol: g.protocol,
                clientId: g.id,
                topic: eff.topic
            };
            // prune undefined for stable JSON comparison
            Object.keys(shape).forEach(k => (shape[k] === undefined) && delete shape[k]);
            return shape;
        }

        function safeStatus(s) {
            try {
                node.status(s);
            } catch (ex) {
                // ignore
            }
        }

        function resolveCfg(msg) {
            const mcfg = msg.kafkaCfg || {};
            const brokerOptions = (node.broker && node.broker.options) || {};

            const groupOpts = {
                groupId: mcfg.groupId || node.groupid || "default-group",
                fromOffset: mcfg.fromOffset || node.fromOffset,
                outOfRangeOffset: mcfg.outOfRangeOffset || node.outOfRangeOffset,
                fetchMinBytes: mcfg.minbytes || node.minbytes,
                fetchMaxBytes: mcfg.maxbytes || node.maxbytes,
                encoding: mcfg.encoding || node.encoding,
                sessionTimeout: mcfg.sessionTimeout,
                protocol: mcfg.protocol,
                id: mcfg.id
            };

            const clientOpts = Object.assign({}, brokerOptions, mcfg.kafkaClientOptions || {});
            const normalizedHost = normalizeKafkaHost(mcfg, brokerOptions);
            if (normalizedHost) clientOpts.kafkaHost = normalizedHost;
            Object.keys(clientOpts).forEach(k => (clientOpts[k] === undefined) && delete clientOpts[k]);

            const topic = msg.topic || node.topic || mcfg.topic;

            return { groupOpts, clientOpts, topic };
        }

        function destroyConsumer() {
            if (node._group) {
                try {
                    node._group.close(true, () => {});
                } catch (ex) {}
                node._group = null;
                node._ready = false;
                safeStatus({});
            }
        }

        function ensureConsumer(eff) {
            const nextShape = stableShape(eff);
            if (!node._ready || !shallowEqual(node._activeCfg, nextShape)){
                destroyConsumer();
                node._activeCfg = nextShape;
                node._gen += 1;
                const myGen = node._gen;
                try {
                    node._group = new ConsumerGroup(Object.assign({}, eff.groupOpts, { kafkaHost: eff.clientOpts.kafkaHost }), eff.topic);
                } catch (e){
                    safeStatus({ fill:'red', shape:'ring', text:'init failed' });
                    node.warn(e);
                    // revert gen increment so next message can try to re-init
                    node._gen -= 1;
                    return false;
                }
                node._group.on('connect', () => {
                    if (myGen !== node._gen) return; // stale
                    node._ready = true;
                    node._lastMessageTs = Date.now();
                    safeStatus({ fill:'green', shape:'dot', text:'Ready' });
                });
                node._group.on('message', (message) => {
                    if (myGen !== node._gen) return; // stale
                    node._lastMessageTs = Date.now();
                    node.status({ fill:'blue', shape:'dot', text:'Reading' });
                    node.send({ payload: message });
                });
                node._group.on('error', (err) => {
                    if (myGen !== node._gen) return; // stale
                    node._ready = false;
                    node._lastMessageTs = null;
                    safeStatus({ fill:'red', shape:'ring', text: (err && err.message) ? err.message : 'Error' });
                    node.warn(err);
                });
                node._group.on('offsetOutOfRange', (err) => {
                    if (myGen !== node._gen) return; // stale
                    safeStatus({ fill:'yellow', shape:'ring', text:'offsetOutOfRange' });
                    node.warn(err);
                });
            }
            return true;
        }

        node.on('input', function(msg) {
            const eff = resolveCfg(msg);
            if (!eff.topic) {
                node.warn("No topic specified");
                return;
            }
            if (!ensureConsumer(eff)) {
                return;
            }
        });

        node.on('close', function() {
            destroyConsumer();
            if (node._interval) {
                clearInterval(node._interval);
                node._interval = null;
            }
        });
    }

    RED.nodes.registerType("zkafka-consumer", KafkaConsumerNode);
};