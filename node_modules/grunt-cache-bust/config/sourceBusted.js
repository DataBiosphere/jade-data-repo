module.exports = {
    options: {
        assets: ['assets/**'],
        baseDir: 'tmp/sourceBusted',
        deleteOriginals: true,
        hash: '123456789',
    },
    files: [{
        src: ['tmp/sourceBusted/assets/css/application.css', 'tmp/sourceBusted/sourceBusted.html']
    }]
};
