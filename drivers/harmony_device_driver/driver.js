'use strict';

const Homey = require('homey');

class HarmonyDeviceDriver extends Homey.Driver {

    async onInit() {
        console.log('Harmony device driver initializing...');
    }

    async onPair(session) {
        console.log('Harmony device driver: onPair started');

        session.setHandler('list_devices', async () => {
            console.log('DeviceDriver: list_devices started');

            const hub = await this.homey.app.resolvePairingHub();
            if (!hub || !hub.ip) {
                throw new Error('No Harmony Hub found.');
            }

            console.log(`DeviceDriver: listing devices for ${hub.ip}`);
            return this.homey.app.getHubDevices(hub.ip, hub.uuid);
        });
    }

}

module.exports = HarmonyDeviceDriver;
