const EventEmitter = require('events');
const { deviceEventKey } = require('./hubidentity.js');

class HubDevice extends EventEmitter {

    constructor(device, parent) {
        super();

        this.id = device.id;
        this.label = device.label;
        this.type = device.type;
        this.controlGroup = device.controlGroup;
        this.model = device.model;
        this.manufacturer = device.manufacturer;
        this.parent = parent;
        this.power = '';
        this._onDeviceStateChanged = this.handleDeviceStateChanged.bind(this);
        this.parent.on(`deviceStateChanged_${String(this.id)}`, this._onDeviceStateChanged);

        parent.parent.emit(`deviceInitialized_${deviceEventKey(parent.canonicalId(), this.id)}`, this);
    }

    handleDeviceStateChanged(deviceState) {
        if (!deviceState || typeof deviceState !== 'object')
            return;

        this.power = deviceState.Power;
        this.emit('stateChanged', deviceState);
    }

}

module.exports = HubDevice
