module.exports = {
    // env: {
    //     test: {
    //         presets: [
    //             "react-app"
    //         ],
    //     },
    // },
    presets: [
        '@babel/preset-env',
        '@babel/preset-react',
        { runtime: 'automatic' },
        "@babel/typescript",
    ],
}
