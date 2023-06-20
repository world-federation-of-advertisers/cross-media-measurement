const path = require('path');
module.exports = {
    paths: function (paths, env) {
        paths.appPublic = path.resolve(__dirname, 'static');
        paths.appHtml = path.resolve(__dirname, 'static/index.html');
        return paths;
    }
}
