This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app) and
then Bazelified.
# Main Scripts

Just like with stock create-react-app, we have the same developer workflow. In the project directory, you can run:

## Bundle the app
`bazel build //src/main/react/reporting-ui:webpack_bundle`

This bundles the app into a `app.bundle.js` file to be used in deployment alongside the index.html file.

## Local deployment
`bazel run //src/main/react/reporting-ui:webpack_devserver`

This runs the app locally. The previous bundling step is not necessary since webpack will manage the source code. This also makes it a little easier to debug since the app isn't minified (it is still converted to JS).

# Additional Tools

## Webpack
We are using webpack to have more control over bundling the app. Other tools like `react-scripts` make some assumptions of the directory structure which don't work with this monorepo. We also have more control with Babel as a transpiler.

Webpack also lets us add plugins to perform tasks like minification/uglification for bundling.

## Babel
This gives us more control over transpiling React/Typescript (TSX) into browser supported JS (ECMA). Babel lets us configure specific loaders which is helpful since we have both React and Typescript in addition to CSS.
