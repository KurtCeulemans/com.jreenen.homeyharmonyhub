'use strict';
const EventEmitter = require('events');
const dgram = require('dgram');
const http = require('http');
const https = require('https');
const { URL } = require('url');
const os = require('os');
const net = require('net');

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

        this.Listener = this._getListener();
        this.broadcastSocket = null;
        this.broadcastInterval = null;
        this.ssdpSocket = null;
        this.ssdpInterval = null;
    }

    start() {
        if (!this.broadcastSocket) {
            this._getBroadcastSocket();
        }
        if (!this.ssdpSocket) {
            this._getSsdpSocket();
        }
    }

    _getBroadcastSocket() {
        const socket = dgram.createSocket('udp4');
        this.broadcastSocket = socket;

        socket.on('error', (err) => {
            console.log(`discovery.js: Socket error ${err}`);
            socket.close();
        });

        socket.on('listening', () => {
            socket.setBroadcast(true);

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

    _getSsdpSocket() {
        const socket = dgram.createSocket({ type: 'udp4', reuseAddr: true });
        this.ssdpSocket = socket;

        socket.on('error', (err) => {
            console.log(`discovery.js SSDP socket error ${err}`);
            socket.close();
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
        if (!location)
            return;

        const hubInfo = {
            ip: rinfo.address,
            uuid: headers.USN ? headers.USN.split(':')[1] : undefined,
            friendlyName: undefined,
            remoteId: undefined,
            hubId: undefined,
        };

        this._fetchSsdpDescription(location).then((desc) => {
            if (desc.friendlyName) hubInfo.friendlyName = desc.friendlyName;
            if (desc.udn && !hubInfo.uuid) hubInfo.uuid = desc.udn.replace('uuid:', '');
            if (hubInfo.ip !== undefined && hubInfo.uuid !== undefined) {
                this._hubManager.addHub(hubInfo);
                this.emit('hubconnected', hubInfo);
            }
        }).catch((err) => {
            console.log(`discovery.js: SSDP description fetch failed ${err}`);
        });
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
            const client = url.protocol === 'https:' ? https : http;
            client.get(url, (res) => {
                res.on('data', (chunk) => content.push(chunk));
                res.on('end', () => {
                    const body = Buffer.concat(content).toString('utf8');
                    const friendlyMatch = body.match(/<friendlyName>([^<]*)<\/friendlyName>/);
                    const udnMatch = body.match(/<UDN>([^<]*)<\/UDN>/i);
                    resolve({
                        friendlyName: friendlyMatch ? friendlyMatch[1] : undefined,
                        udn: udnMatch ? udnMatch[1] : undefined,
                    });
                });
            }).on('error', (err) => reject(err));
        });
    }

    discoverHubByIp(ip) {
        return this._probeHubIp(ip).then((hubInfo) => {
            this._hubManager.addHub(hubInfo);
            this.emit('hubconnected', hubInfo);
            return hubInfo;
        });
    }

    _probeHubIp(ip) {
        return new Promise((resolve, reject) => {
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

                res.on('data', (chunk) => {
                    responseBody += chunk;
                });

                res.on('end', () => {
                    try {
                        const data = JSON.parse(responseBody);
                        const hubData = data.data;
                        if (!hubData || !hubData.activeRemoteId) {
                            return reject(new Error('No activeRemoteId returned'));
                        }

                        const remoteId = hubData.activeRemoteId;
                        const friendlyName = hubData.activeRemoteName || `Harmony Hub ${ip}`;
                        resolve({
                            ip,
                            remoteId,
                            hubId: remoteId,
                            uuid: remoteId,
                            friendlyName
                        });
                    } catch (error) {
                        reject(error);
                    }
                });
            });

            req.on('error', reject);
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

                const hubInfo = this._deserializeHubInfo(buffer);
                if (hubInfo.ip !== undefined) {
                    this._hubManager.addHub(hubInfo);
                    this.emit('hubconnected', hubInfo);
                }
            };

            client.on('end', processBuffer);
            client.on('close', processBuffer);
        });
        server.on('error', (err) => {
            console.log(err);
            throw err;
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

    _getLocalIp() {
        const ifaces = os.networkInterfaces();
        let address = '0.0.0.0';

        Object.keys(ifaces).forEach(function(ifname) {
            ifaces[ifname].forEach(function(iface) {
                if (iface.family === 'IPv4' && iface.internal === false)
                    address = iface.address;

            });
        });

        return address;
    }

}
module.exports = Discovery;
