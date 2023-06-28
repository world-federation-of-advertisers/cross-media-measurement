module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'jsdom',
    transform: {
        '^.+\\.(ts|tsx)$': 'ts-jest',
    },
    moduleFileExtensions: ['ts', 'js', 'tsx'],
    moduleNameMapper: {
        "\\.(css|scss)$": "<rootDir>/../jest/__mocks__/styleMock.js"
    },
    testMatch: [
        '<rootDir>/**/*.test.(ts|tsx)'
    ],
}
