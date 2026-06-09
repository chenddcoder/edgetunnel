"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const tslib_1 = require("tslib");
const http_1 = require("http");
const url_1 = require("url");
const ws_1 = require("ws");
const utils_1 = require("./app/utils");
const uuid_1 = require("uuid");
const node_fs_1 = require("node:fs");
const node_dns_1 = require("node:dns");
const node_dgram_1 = require("node:dgram");
const vless_js_1 = require("vless-js");
const node_net_1 = require("node:net");
const stream_1 = require("stream");
const web_1 = require("node:stream/web");
const port = process.env.PORT;
const smallRAM = process.env.SMALLRAM || false;
const userID = process.env.UUID || '';
//'ipv4first' or 'verbatim'
const dnOder = process.env.DNSORDER || 'verbatim';
if (dnOder === 'ipv4first') {
    (0, node_dns_1.setDefaultResultOrder)(dnOder);
}
let isVaildUser = (0, uuid_1.validate)(userID);
if (!isVaildUser) {
    console.log('not set valid UUID');
}
const server = (0, http_1.createServer)((req, resp) => {
    var _a;
    if (!isVaildUser) {
        return (0, utils_1.index401)(req, resp);
    }
    const url = new URL(req.url, `http://${req.headers['host']}`);
    // health check
    if (req.method === 'GET' && url.pathname.startsWith('/health')) {
        resp.writeHead(200);
        resp.write('health 200');
        resp.end();
        return;
    }
    // index page
    if (url.pathname.includes(userID)) {
        const index = 'dist/apps/cf-page-vless/index.html';
        resp.writeHead(200, {
            'Content-Type': 'text/html,charset=UTF-8',
        });
        return (0, node_fs_1.createReadStream)(index).pipe(resp);
    }
    if (req.method === 'GET' && url.pathname.startsWith('/assets')) {
        return (0, utils_1.serverStaticFile)(req, resp);
    }
    const basicAuth = req.headers.authorization || '';
    const authStringBase64 = ((_a = basicAuth.split(' ')) === null || _a === void 0 ? void 0 : _a[1]) || '';
    const authString = Buffer.from(authStringBase64, 'base64').toString('ascii');
    if (authString && authString.includes(userID)) {
        resp.writeHead(302, {
            'content-type': 'text/html; charset=utf-8',
            Location: `./${userID}`,
        });
        resp.end();
    }
    else {
        resp.writeHead(401, {
            'content-type': 'text/html; charset=utf-8',
            'WWW-Authenticate': 'Basic',
        });
        resp.end();
    }
});
const vlessWServer = new ws_1.WebSocketServer({ noServer: true });
vlessWServer.on('connection', function connection(ws, request) {
    return tslib_1.__awaiter(this, void 0, void 0, function* () {
        let address = '';
        let portWithRandomLog = '';
        try {
            const log = (info, event) => {
                console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
            };
            let remoteConnection = null;
            let udpClientStream = null;
            let remoteConnectionReadyResolve;
            const earlyDataHeader = request.headers['sec-websocket-protocol'];
            const readableWebSocketStream = (0, vless_js_1.makeReadableWebSocketStream)(ws, earlyDataHeader, log);
            let vlessResponseHeader = null;
            // ws  --> remote
            readableWebSocketStream
                .pipeTo(new web_1.WritableStream({
                write(chunk, controller) {
                    return tslib_1.__awaiter(this, void 0, void 0, function* () {
                        if (!Buffer.isBuffer(chunk)) {
                            chunk = Buffer.from(chunk);
                        }
                        if (udpClientStream) {
                            const writer = udpClientStream.writable.getWriter();
                            // nodejs buffer to ArrayBuffer issue
                            // https://nodejs.org/dist/latest-v18.x/docs/api/buffer.html#bufbuffer
                            yield writer.write(chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length));
                            writer.releaseLock();
                            return;
                        }
                        if (remoteConnection) {
                            yield socketAsyncWrite(remoteConnection, chunk);
                            // remoteConnection.write(chunk);
                            return;
                        }
                        const vlessBuffer = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length);
                        const { hasError, message, portRemote, addressRemote, rawDataIndex, vlessVersion, isUDP, } = (0, vless_js_1.processVlessHeader)(vlessBuffer, userID);
                        address = addressRemote || '';
                        portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '} `;
                        if (hasError) {
                            controller.error(`[${address}:${portWithRandomLog}] ${message} `);
                            return;
                        }
                        // const addressType = requestAddr >> 42
                        // const addressLength = requestAddr & 0x0f;
                        console.log(`[${address}:${portWithRandomLog}] connecting`);
                        vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
                        const rawClientData = vlessBuffer.slice(rawDataIndex);
                        if (isUDP) {
                            // 如果仅仅是针对DNS， 这样是没有必要的。因为xray 客户端 DNS A/AAA query 都有长度 header，
                            // 所以直接和 DNS server over TCP。所以无需 runtime 支持 UDP API。
                            // DNS over UDP 和 TCP 唯一的区别就是 Header section format 多了长度
                            //  https://www.rfc-editor.org/rfc/rfc1035#section-4.2.2
                            udpClientStream = makeUDPSocketStream(portRemote, address);
                            const writer = udpClientStream.writable.getWriter();
                            writer.write(rawClientData).catch((error) => console.log);
                            writer.releaseLock();
                            remoteConnectionReadyResolve(udpClientStream);
                        }
                        else {
                            remoteConnection = yield connect2Remote(portRemote, address, log);
                            remoteConnection.write(new Uint8Array(rawClientData));
                            remoteConnectionReadyResolve(remoteConnection);
                        }
                    });
                },
                close() {
                    // if (udpClientStream ) {
                    //   udpClientStream.writable.close();
                    // }
                    // (remoteConnection as Socket).end();
                    console.log(`[${address}:${portWithRandomLog}] readableWebSocketStream is close`);
                },
                abort(reason) {
                    // TODO: log can be remove, abort will catch by catch block
                    console.log(`[${address}:${portWithRandomLog}] readableWebSocketStream is abort`, JSON.stringify(reason));
                },
            }))
                .catch((error) => {
                console.error(`[${address}:${portWithRandomLog}] readableWebSocketStream pipeto has exception`, error.stack || error);
                // error is cancel readable stream anyway, no need close websocket in here
                // closeWebSocket(webSocket);
                // close remote conn
                // remoteConnection?.close();
            });
            yield new Promise((resolve) => (remoteConnectionReadyResolve = resolve));
            // remote --> ws
            let responseStream = udpClientStream === null || udpClientStream === void 0 ? void 0 : udpClientStream.readable;
            if (remoteConnection) {
                // ignore type error
                // @ts-ignore
                responseStream = stream_1.Readable.toWeb(remoteConnection, {
                    strategy: {
                        // due to nodejs issue https://github.com/nodejs/node/issues/46347
                        highWaterMark: smallRAM ? 100 : 1000, // 1000 * tcp mtu(64kb) = 64mb
                    },
                });
            }
            let count = 0;
            // ws.send(vlessResponseHeader!);
            // remoteConnection.pipe(
            //   new Writable({
            //     async write(chunk: Uint8Array, encoding, callback) {
            //       count += chunk.byteLength;
            //       console.log('ws write', count / (1024 * 1024));
            //       console.log(
            //         '-----++++',
            //         (remoteConnection as Socket).bytesRead / (1024 * 1024)
            //       );
            //       if (ws.readyState === ws.OPEN) {
            //         await wsAsyncWrite(ws, chunk);
            //         callback();
            //       }
            //     },
            //   })
            // );
            // if readable not pipe can't wait fro writeable write method
            yield responseStream.pipeTo(new web_1.WritableStream({
                start() {
                    if (ws.readyState === ws.OPEN) {
                        ws.send(vlessResponseHeader);
                    }
                },
                write(chunk, controller) {
                    return tslib_1.__awaiter(this, void 0, void 0, function* () {
                        // count += chunk.byteLength;
                        // console.log('ws write', count / (1024 * 1024));
                        // console.log(
                        //   '-----++++',
                        //   (remoteConnection as Socket).bytesRead / (1024 * 1024),
                        //   (remoteConnection as Socket).readableHighWaterMark
                        // );
                        // we have issue there, maybe beacsue nodejs web stream has bug.
                        // socket web stream will read more data from socket
                        if (ws.readyState === ws.OPEN) {
                            yield wsAsyncWrite(ws, chunk);
                        }
                        else {
                            if (!remoteConnection.destroyed) {
                                remoteConnection.destroy();
                            }
                        }
                    });
                },
                close() {
                    console.log(`[${address}:${portWithRandomLog}] remoteConnection!.readable is close`);
                },
                abort(reason) {
                    (0, vless_js_1.closeWebSocket)(ws);
                    console.error(`[${address}:${portWithRandomLog}] remoteConnection!.readable abort`, reason);
                },
            }));
        }
        catch (error) {
            console.error(`[${address}:${portWithRandomLog}] processWebSocket has exception `, error.stack || error);
            (0, vless_js_1.closeWebSocket)(ws);
        }
    });
});
server.on('upgrade', function upgrade(request, socket, head) {
    const { pathname } = (0, url_1.parse)(request.url);
    vlessWServer.handleUpgrade(request, socket, head, function done(ws) {
        vlessWServer.emit('connection', ws, request);
    });
});
server.listen({
    port: port,
    host: '0.0.0.0',
    // host: '0.0.0.0',
}, () => {
    console.log(`server listen in http://127.0.0.1:${port}`);
});
function connect2Remote(port, host, log) {
    return tslib_1.__awaiter(this, void 0, void 0, function* () {
        return new Promise((resole, reject) => {
            const remoteSocket = (0, node_net_1.connect)({
                port: port,
                host: host,
                // https://github.com/nodejs/node/pull/46587
                // autoSelectFamily: true,
            }, () => {
                log(`connected`);
                resole(remoteSocket);
            });
            remoteSocket.addListener('error', () => {
                reject('remoteSocket has error');
            });
        });
    });
}
function socketAsyncWrite(ws, chunk) {
    return tslib_1.__awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => {
            ws.write(chunk, (error) => {
                if (error) {
                    reject(error);
                }
                else {
                    resolve('');
                }
            });
        });
    });
}
function wsAsyncWrite(ws, chunk) {
    return tslib_1.__awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => {
            ws.send(chunk, (error) => {
                if (error) {
                    reject(error);
                }
                else {
                    resolve('');
                }
            });
        });
    });
}
function makeUDPSocketStream(portRemote, address) {
    const udpClient = (0, node_dgram_1.createSocket)('udp4');
    const transformStream = new web_1.TransformStream({
        start(controller) {
            /* … */
            udpClient.on('message', (message, info) => {
                // console.log(
                //   `udp package received ${info.size} bytes from ${info.address}:${info.port}`,
                //   Buffer.from(message).toString('hex')
                // );
                controller.enqueue(Buffer.concat([
                    new Uint8Array([(info.size >> 8) & 0xff, info.size & 0xff]),
                    message,
                ]));
            });
            udpClient.on('error', (error) => {
                console.log('udpClient error event', error);
                controller.error(error);
            });
        },
        transform(chunk, controller) {
            return tslib_1.__awaiter(this, void 0, void 0, function* () {
                //seems v2ray will use same web socket for dns query..
                //And v2ray will combine A record and AAAA record into one ws message and use 2 btye for dns query length
                for (let index = 0; index < chunk.byteLength;) {
                    const lengthBuffer = chunk.slice(index, index + 2);
                    const udpPakcetLength = new DataView(lengthBuffer).getInt16(0);
                    const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPakcetLength));
                    index = index + 2 + udpPakcetLength;
                    yield new Promise((resolve, reject) => {
                        udpClient.send(udpData, portRemote, address, (err) => {
                            if (err) {
                                console.log('udps send error', err);
                                controller.error(`Failed to send UDP packet !! ${err}`);
                                safeCloseUDP(udpClient);
                            }
                            // console.log(
                            //   'udp package sent',
                            //   Buffer.from(udpData).toString('hex')
                            // );
                            resolve(true);
                        });
                    });
                    index = index;
                }
                // console.log('dns chunk', chunk);
                // console.log(portRemote, address);
                // port is big-Endian in raw data etc 80 == 0x005d
            });
        },
        flush(controller) {
            safeCloseUDP(udpClient);
            controller.terminate();
        },
    });
    return transformStream;
}
function safeCloseUDP(client) {
    try {
        client.close();
    }
    catch (error) {
        console.log('error close udp', error);
    }
}
//# sourceMappingURL=main.js.map