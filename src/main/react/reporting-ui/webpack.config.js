const path = require('path');
const webpack = require('webpack');
const MiniCssExtractPlugin = require("mini-css-extract-plugin")


module.exports = {
  entry: path.resolve(__dirname, './src/index.tsx'),
  module: {
    rules: [
      {
        test: /\.(js|jsx)$/,
        exclude: /node_modules/,
        use: ['babel-loader'],
      },
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
      {
        test: /\.css$/i,
        use: ["style-loader", "css-loader"],
      },
    ],
  },
  resolve: {
    extensions: ['.js', '.jsx', '.ts', '.tsx', '.css'],
    modules: [
      'node_modules'
    ],
  },
  output: {
    path: path.resolve(__dirname, './public'),
    filename: 'index.js',
  },
  plugins: [
    new MiniCssExtractPlugin(),
  ],
  devServer: {
    static: path.resolve(__dirname, './public'),
  },
};