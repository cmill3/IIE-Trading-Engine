class ThrowErrorPlugin {
    constructor(serverless, options) {
      this.serverless = serverless;
      this.hooks = {
        'before:package:initialize': this.throwError.bind(this),
      };
    }
  
    throwError() {
      const message = this.serverless.service.custom.throwError;
      if (message) {
        throw new Error(message);
      }
    }
  }
  
  module.exports = ThrowErrorPlugin;
  