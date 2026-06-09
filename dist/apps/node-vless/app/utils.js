"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.serverIndexPage = exports.index401 = exports.serverStaticFile = void 0;
const node_fs_1 = require("node:fs");
const node_path_1 = require("node:path");
const pretty_cache_header_1 = require("pretty-cache-header");
const mimeLookup = {
    '.js': 'application/javascript,charset=UTF-8',
    '.html': 'text/html,charset=UTF-8',
    '.css': 'text/css; charset=UTF-8',
};
const staticPath = 'dist/apps/cf-page-vless/';
const file401 = 'dist/apps/node-vless/assets/401.html';
let filepath = null;
function serverStaticFile(req, resp) {
    const url = new URL(req.url, `http://${req.headers['host']}`);
    let fileurl = url.pathname;
    fileurl = (0, node_path_1.join)(staticPath, fileurl);
    console.log('....', fileurl);
    filepath = (0, node_path_1.resolve)(fileurl);
    console.log(filepath);
    if ((0, node_fs_1.existsSync)(filepath)) {
        let fileExt = (0, node_path_1.extname)(filepath);
        console.log('fileExt', fileExt);
        let mimeType = mimeLookup[fileExt];
        resp.writeHead(200, {
            'Content-Type': mimeType,
            'Cache-Control': (0, pretty_cache_header_1.cacheHeader)({
                public: true,
                maxAge: '1year',
                staleWhileRevalidate: '1year',
            }),
        });
        return (0, node_fs_1.createReadStream)(filepath).pipe(resp);
    }
    else {
        resp.writeHead(404);
        resp.write('not found');
        resp.end();
        return resp;
    }
}
exports.serverStaticFile = serverStaticFile;
function index401(req, resp) {
    const file401Path = (0, node_path_1.resolve)(file401);
    if ((0, node_fs_1.existsSync)(file401Path)) {
        (0, node_fs_1.createReadStream)(file401Path).pipe(resp);
    }
    else {
        resp.writeHead(401);
        resp.write('UUID env not set');
        resp.end();
    }
}
exports.index401 = index401;
function serverIndexPage(req, resp, uuid) {
    // if()
}
exports.serverIndexPage = serverIndexPage;
//# sourceMappingURL=utils.js.map