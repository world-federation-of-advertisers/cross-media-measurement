module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'jsdom',
    transform: {
        '^.+\\.(ts|tsx|jsx)$': 'ts-jest',
    },
    moduleFileExtensions: ['ts', 'js', 'tsx'],
    moduleNameMapper: {
        "\\.(css|scss)$": "<rootDir>../jest/__mocks__/styleMock.js"
    },
    testMatch: [
        '<rootDir>/**/*.test.(js|ts|jsx|tsx)'
    ],
}
