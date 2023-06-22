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
const DtsBundleWebpack = require('dts-bundle-webpack');

module.exports = {
  entry: path.resolve(__dirname, './src/index.ts'),
  module: {
    rules: [
      {
        test: /\.(js|jsx)$/,
        exclude: /node_modules/,
        use: ['babel-loader'],
      },
      {
        test: /\.(ts|tsx)$/,
        use: 'ts-loader',
        exclude: /node_modules/,
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
    path: path.resolve(__dirname, './dist'),
    filename: 'ocmm-comp-lib.js',
    publicPath: '/',
  },
  plugins: [
    new MiniCssExtractPlugin(),
    new DtsBundleWebpack({
      name: 'ocmm-comp-lib',
      main: 'src/main/react/reporting-ui-component-library/dist/index.d.ts', // for some reason this is from the WORKSPACE root
    }),
  ],
};
