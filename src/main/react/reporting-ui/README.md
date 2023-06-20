This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).
Then Bazel configuration was added in `WORKSPACE` and `BUILD.bazel`

See the [examples guide](https://bazelbuild.github.io/rules_nodejs/examples#react) for a comparison of the several
approaches you can take to build and test your React app with Bazel.

## Available Scripts

Just like with stock create-react-app, we have the same developer workflow. In the project directory, you can run:

### `yarn start`

Runs the app in the development mode.<br />
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.<br />
You will also see any lint errors in the console.

### `yarn test`

Launches the test runner in the interactive watch mode.<br />
Note that it restarts Jest on each change, so there's a large time penalty on each re-run.
This can be solved by making Jest ibazel-aware as we did with Karma.
See https://github.com/bazelbuild/rules_nodejs/issues/2028

### `yarn build`

Builds the app for production to the `build` folder.<br />
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.<br />
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

