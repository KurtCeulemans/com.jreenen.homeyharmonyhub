'use strict';

const Homey = require('homey');

class HarmonyDeviceDriver extends Homey.Driver {

    async onInit() {
        this.log('Harmony device driver initializing...');

        this.homey.flow.getConditionCard('is_on')
            .registerRunListener(async (args) => {
                const device = args.device;
                if (!device || !device.device)
                    return false;

                return device.device.power === 'On';
            });
    }

    async onPair(session) {
        this.log('Harmony device driver: onPair started');

        session.setHandler('list_devices', async () => {
            this.log('DeviceDriver: list_devices started');

            const hub = await this.homey.app.resolvePairingHub();
            if (!hub || !hub.ip)
                throw new Error('No Harmony Hub found.');

            this.log(`DeviceDriver: listing devices for ${hub.ip}`);
            return this.homey.app.getHubDevices(hub.ip, hub.remoteId || hub.uuid);
        });
    }

}

module.exports = HarmonyDeviceDriver;
