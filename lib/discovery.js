'use strict';
const EventEmitter = require('events');
const dgram = require('dgram');
const http = require('http');
const https = require('https');
const { URL } = require('url');
const net = require('net');
const { normalizeHubInfo } = require('./hubidentity.js');

const SSDP_FETCH_TIMEOUT_MS = 3000;
const SSDP_MAX_BODY_BYTES = 64 * 1024;
const SOCKET_RESTART_MS = 5000;

class Discovery extends EventEmitter {

    constructor(hubManager, homey) {
        super();
        this.homey = homey;
        this._hubManager = hubManager;
        this.LISTENER_PORT = 5446;
        this.MULTICAST_ADDR = '255.255.255.255';
        this.MULTICAST_PORT = 5224;
        this.PING_INTERVAL = 2000;
        this.SSDP_ADDR = '239.255.255.250';
        this.SSDP_PORT = 1900;
        this.SSDP_ST = 'urn:myharmony-com:device:harmony:1';
        this.SSDP_ENRICHMENT_TIMEOUT = 3000;
        this._ssdpEnrichmentPending = new Set();
        this._ssdpEnrichedIps = new Set();

        this.Listener = this._getListener();
        this.broadcastSocket = null;
        this.broadcastInterval = null;
        this.broadcastRestartTimer = null;
        this.ssdpSocket = null;
        this.ssdpInterval = null;
        this.ssdpRestartTimer = null;
    }

    start() {
        if (!this.broadcastSocket)
            this._getBroadcastSocket();

        if (!this.ssdpSocket)
            this._getSsdpSocket();
    }

    _getBroadcastSocket() {
        const socket = dgram.createSocket('udp4');
        this.broadcastSocket = socket;

        socket.on('error', (err) => {
            console.log(`discovery.js: Socket error ${err}`);
            this._teardownBroadcast();
            this._scheduleBroadcastRestart();
        });

        socket.on('listening', () => {
            try {
                socket.setBroadcast(true);
            } catch (err) {
                console.log(`discovery.js: setBroadcast error ${err}`);
            }

            const sendSearch = (target) => {
                const data = '_logitech-reverse-bonjour._tcp.local.\n' + this.LISTENER_PORT;
                const search = Buffer.from(data, 'ascii');
                const address = target || this.MULTICAST_ADDR;

                console.log(`discovery.js: sending discovery to ${address}:${this.MULTICAST_PORT}`);
                try {
                    socket.send(search, 0, search.length, this.MULTICAST_PORT, address);
                } catch (ex) {
                    console.log(ex);
                }
            };

            sendSearch();
            this.broadcastInterval = this.homey.setInterval(() => sendSearch(), this.PING_INTERVAL);
        });

        socket.bind(this.MULTICAST_PORT, '0.0.0.0');
    }

    _teardownBroadcast() {
        if (this.broadcastInterval) {
            this.homey.clearInterval(this.broadcastInterval);
            this.broadcastInterval = null;
        }

        const socket = this.broadcastSocket;
        this.broadcastSocket = null;
        if (!socket)
            return;

        socket.removeAllListeners();
        try {
            socket.close();
        } catch (err) {
            console.log(`discovery.js: broadcast close error ${err}`);
        }
    }

    _scheduleBroadcastRestart() {
        if (this.broadcastRestartTimer)
            return;

        this.broadcastRestartTimer = this.homey.setTimeout(() => {
            this.broadcastRestartTimer = null;
            if (!this.broadcastSocket)
                this._getBroadcastSocket();
        }, SOCKET_RESTART_MS);
    }

    _getSsdpSocket() {
        const socket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        this.ssdpSocket = socket;

        socket.on('error', (err) => {
            console.log(`discovery.js SSDP socket error ${err}`);
            this._teardownSsdp();
            this._scheduleSsdpRestart();
        });

        socket.on('message', (msg, rinfo) => {
            this._handleSsdpResponse(msg.toString(), rinfo);
        });

        socket.on('listening', () => {
            try {
                socket.addMembership(this.SSDP_ADDR);
            } catch (err) {
                console.log(`discovery.js SSDP addMembership error ${err}`);
            }

            const sendSearch = () => {
                const search = '' +
                    'M-SEARCH * HTTP/1.1\r\n' +
                    `HOST: ${this.SSDP_ADDR}:${this.SSDP_PORT}\r\n` +
                    'MAN: "ssdp:discover"\r\n' +
                    'MX: 3\r\n' +
                    `ST: ${this.SSDP_ST}\r\n` +
                    '\r\n';

                console.log(`discovery.js: sending SSDP search to ${this.SSDP_ADDR}:${this.SSDP_PORT}`);
                socket.send(Buffer.from(search, 'ascii'), 0, search.length, this.SSDP_PORT, this.SSDP_ADDR, (err) => {
                    if (err) console.log(`discovery.js SSDP send error ${err}`);
                });
            };

            sendSearch();
            this.ssdpInterval = this.homey.setInterval(sendSearch, 10000);
        });

        socket.bind(0, '0.0.0.0');
    }

    _teardownSsdp() {
        if (this.ssdpInterval) {
            this.homey.clearInterval(this.ssdpInterval);
            this.ssdpInterval = null;
        }

        const socket = this.ssdpSocket;
        this.ssdpSocket = null;
        if (!socket)
            return;

        socket.removeAllListeners();
        try {
            socket.close();
        } catch (err) {
            console.log(`discovery.js: SSDP close error ${err}`);
        }
    }

    _scheduleSsdpRestart() {
        if (this.ssdpRestartTimer)
            return;

        this.ssdpRestartTimer = this.homey.setTimeout(() => {
            this.ssdpRestartTimer = null;
            if (!this.ssdpSocket)
                this._getSsdpSocket();
        }, SOCKET_RESTART_MS);
    }

    _handleSsdpResponse(response, rinfo) {
        if (!response.startsWith('HTTP/1.1 200 OK'))
            return;

        const headers = {};
        response.split('\r\n').forEach((line) => {
            const index = line.indexOf(':');
            if (index > 0) {
                const key = line.slice(0, index).trim().toUpperCase();
                const value = line.slice(index + 1).trim();
                headers[key] = value;
            }
        });

        if (!headers.ST || headers.ST !== this.SSDP_ST)
            return;

        const location = headers.LOCATION;
        if (!location || !this._isSafeSsdpLocation(location, rinfo))
            return;

        if (this._ssdpEnrichmentPending.has(rinfo.address) || this._ssdpEnrichedIps.has(rinfo.address))
            return;

        this._ssdpEnrichmentPending.add(rinfo.address);

        this._fetchSsdpDescription(location).then((desc) => {
            return this._probeHubIp(rinfo.address, this.SSDP_ENRICHMENT_TIMEOUT).then((probeInfo) => {
                const hubInfo = normalizeHubInfo({
                    ip: rinfo.address,
                    friendlyName: desc.friendlyName || probeInfo.friendlyName,
                    remoteId: probeInfo.remoteId,
                    hubId: probeInfo.hubId,
                    uuid: probeInfo.uuid
                });

                if (!hubInfo.ip || !hubInfo.remoteId)
                    return;

                this._ssdpEnrichedIps.add(hubInfo.ip);
                this._hubManager.addHub(hubInfo);
                this.emit('hubconnected', hubInfo);
            });
        }).catch((err) => {
            console.log(`discovery.js: SSDP hub enrichment failed ${err}`);
        }).finally(() => {
            this._ssdpEnrichmentPending.delete(rinfo.address);
        });
    }

    _isSafeSsdpLocation(location, rinfo) {
        let url;
        try {
            url = new URL(location);
        } catch (err) {
            return false;
        }

        if (url.protocol !== 'http:' && url.protocol !== 'https:')
            return false;

        return url.hostname === rinfo.address;
    }

    _fetchSsdpDescription(location) {
        return new Promise((resolve, reject) => {
            let url;
            try {
                url = new URL(location);
            } catch (err) {
                return reject(err);
            }

            const content = [];
            let received = 0;
            let settled = false;
            const finish = (fn, value) => {
                if (settled)
                    return;
                settled = true;
                fn(value);
            };
            const client = url.protocol === 'https:' ? https : http;
            const req = client.get(url, (res) => {
                res.on('error', (err) => finish(reject, err));
                res.on('data', (chunk) => {
                    received += chunk.length;
                    if (received > SSDP_MAX_BODY_BYTES) {
                        req.destroy();
                        return finish(reject, new Error('SSDP description exceeded size limit'));
                    }
                    content.push(chunk);
                });
                res.on('end', () => {
                    const body = Buffer.concat(content).toString('utf8');
                    const friendlyMatch = body.match(/<friendlyName>([^<]*)<\/friendlyName>/);
                    const udnMatch = body.match(/<UDN>([^<]*)<\/UDN>/i);
                    finish(resolve, {
                        friendlyName: friendlyMatch ? friendlyMatch[1] : undefined,
                        udn: udnMatch ? udnMatch[1] : undefined
                    });
                });
            });

            req.setTimeout(SSDP_FETCH_TIMEOUT_MS, () => {
                req.destroy();
                finish(reject, new Error(`SSDP description timed out after ${SSDP_FETCH_TIMEOUT_MS}ms`));
            });
            req.on('error', (err) => finish(reject, err));
        });
    }

    discoverHubByIp(ip) {
        return this._probeHubIp(ip, 5000).then((hubInfo) => {
            const normalized = normalizeHubInfo(hubInfo);
            this._hubManager.addHub(normalized);
            this.emit('hubconnected', normalized);
            return normalized;
        });
    }

    _probeHubIp(ip, timeoutMs) {
        return new Promise((resolve, reject) => {
            let settled = false;
            const finish = (fn, value) => {
                if (settled)
                    return;
                settled = true;
                fn(value);
            };

            const body = JSON.stringify({
                'id ': 1,
                cmd: 'setup.account?getProvisionInfo',
                params: {}
            });

            const options = {
                hostname: ip,
                port: 8088,
                path: '/',
                method: 'POST',
                headers: {
                    Origin: 'http://sl.dhg.myharmony.com',
                    'Content-Type': 'application/json',
                    Accept: 'application/json',
                    'Accept-Charset': 'utf-8',
                    'Content-Length': Buffer.byteLength(body)
                }
            };

            const req = http.request(options, (res) => {
                let responseBody = '';
                res.setEncoding('utf8');
                res.on('error', (err) => finish(reject, err));

                res.on('data', (chunk) => {
                    responseBody += chunk;
                    if (responseBody.length > SSDP_MAX_BODY_BYTES) {
                        req.destroy();
                        finish(reject, new Error('Hub probe response exceeded size limit'));
                    }
                });

                res.on('end', () => {
                    try {
                        const data = JSON.parse(responseBody);
                        const hubData = data.data;
                        if (!hubData || !hubData.activeRemoteId)
                            return finish(reject, new Error('No activeRemoteId returned'));

                        const remoteId = String(hubData.activeRemoteId);
                        const friendlyName = hubData.activeRemoteName || `Harmony Hub ${ip}`;
                        finish(resolve, normalizeHubInfo({
                            ip,
                            remoteId,
                            hubId: remoteId,
                            uuid: remoteId,
                            friendlyName
                        }));
                    } catch (error) {
                        finish(reject, error);
                    }
                });
            });

            if (timeoutMs)
                req.setTimeout(timeoutMs, () => {
                    req.destroy();
                    finish(reject, new Error(`Hub probe timed out after ${timeoutMs}ms`));
                });

            req.on('error', (err) => finish(reject, err));
            req.write(body);
            req.end();
        });
    }

    _getListener() {
        const server = net.createServer(client => {
            let buffer = '';
            let processed = false;

            client.on('error', err => {
                console.log(`discovery.js error: ${err}`);
            });

            client.on('data', (data) => {
                buffer += data.toString();
            });

            const processBuffer = () => {
                if (processed)
                    return;
                processed = true;

                const rawInfo = this._deserializeHubInfo(buffer);
                if (rawInfo.ip === undefined)
                    return;

                const publish = (hubInfo) => {
                    const normalized = normalizeHubInfo(hubInfo);
                    if (!normalized.remoteId)
                        return;
                    this._hubManager.addHub(normalized);
                    this.emit('hubconnected', normalized);
                };

                if (rawInfo.remoteId || rawInfo.hubId)
                    publish(rawInfo);
                else
                    this._probeHubIp(rawInfo.ip, 5000).then((probeInfo) => {
                        publish(Object.assign({}, rawInfo, probeInfo));
                    }).catch((err) => {
                        console.log(`discovery.js: Bonjour hub enrichment failed ${err}`);
                    });
            };

            client.on('end', processBuffer);
            client.on('close', processBuffer);
        });
        server.on('error', (err) => {
            console.log(`discovery.js: TCP listener error ${err}`);
        });
        server.listen(this.LISTENER_PORT, () => {
            console.log('server bound');
        });

        return server;
    }

    _deserializeHubInfo(response) {
        const pairs = {}

        response.split(';')
            .forEach(function(rawPair) {
                const splitted = rawPair.split(':')
                pairs[splitted[0]] = splitted[1]
            })

        return pairs
    }

}
module.exports = Discovery;
