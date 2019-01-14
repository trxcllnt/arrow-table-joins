module.exports = {
    "verbose": false,
    // "reporters": [
    //   "jest-silent-reporter"
    // ],
    "testEnvironment": "node",
    "globals": {
      "ts-jest": {
        "diagnostics": false,
        "tsConfig": "test/tsconfig.json"
      }
    },
    "roots": [
      "<rootDir>/test/"
    ],
    "moduleFileExtensions": [
      "js",
      "ts",
      "tsx"
    ],
    "coverageReporters": [
      "lcov"
    ],
    "coveragePathIgnorePatterns": [
      "test\\/.*\\.(ts|tsx|js)$",
      "/node_modules/"
    ],
    "transform": {
      "^.+\\.jsx?$": "ts-jest",
      "^.+\\.tsx?$": "ts-jest"
    },
    "transformIgnorePatterns": [
      "/node_modules/(?!web-stream-tools).+\\.js$"
    ],
    "testRegex": "(.*(-|\\.)(test|spec)s?)\\.(ts|tsx|js)$",
    "preset": "ts-jest",
    "testMatch": null
};
