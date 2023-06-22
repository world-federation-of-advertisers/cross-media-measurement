// Copyright 2023 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const path = require('path');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const HtmlWebpackPlugin = require('html-webpack-plugin');

module.exports = {
  entry: path.resolve(__dirname, './src/index.tsx'),
  module: {
    rules: [
      {
        test: /\.(js|jsx)$/,
        use: ['babel-loader'],
        // Needed because the local npm package isn't bundled yet
        include: [/src/, /node_modules\/ocmm-comp-lib/],
      },
      {
        test: /\.(ts|tsx)$/,
        loader: 'ts-loader',
        // Needed because the local npm package isn't bundled yet
        include: [/src/, /node_modules\/ocmm-comp-lib/],
        options: {allowTsInNodeModules: true},
      },
      {
        test: /\.css$/i,
        use: ['style-loader', 'css-loader'],
      },
    ],
  },
  resolve: {
    extensions: ['.js', '.jsx', '.ts', '.tsx', '.css'],
    modules: ['node_modules'],
  },
  output: {
    path: path.resolve(__dirname, './public'),
    filename: 'app.bundle.js',
    publicPath: '/',
  },
  plugins: [
    new MiniCssExtractPlugin(),
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, 'public/index.html'),
      inject: true,
    }),
  ],
  devServer: {
    // static: path.resolve(__dirname, './public'),
    historyApiFallback: true,
  },
};
