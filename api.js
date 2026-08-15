module.exports = {
    async getPairedDevices({ homey }) {
        return homey.app.getPairedDevices();
    },
    async sendDebugReport({ homey }) {
        return homey.app.sendDebugReport();
    }
};
