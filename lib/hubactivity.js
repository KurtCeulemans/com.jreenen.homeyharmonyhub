const EventEmitter = require('events');

class HubActivity extends EventEmitter {

    constructor(activity, parent) {
        super();

        this.id = activity.id;
        this.label = activity.label;
        this.type = activity.type;
        this.fixit = activity.fixit;
        this.controlGroup = activity.controlGroup;
        this.parent = parent;
        this._onActivityChanged = this.handleActivityChanged.bind(this);
        this.parent.on(`activityChangeMessage_${String(this.id)}`, this._onActivityChanged);
    }

    handleActivityChanged(activityMessage, hubId) {
        if (!activityMessage || typeof activityMessage !== 'object')
            return;

        console.log(`Activity change notification received on hub ${hubId}`);
        if (!this.parent._isOwnHub(hubId))
            return;

        const hubKey = this.parent.canonicalId();

        if (String(this.id) === '-1' && activityMessage.activityStatus === 0 && activityMessage.runningActivityList === '') {
            this.parent.emit('currentActivityChanged', this, hubKey);
            this.parent.parent.emit('activityChanged', this.label, hubKey);
            this._emitFixitStates();
        }

        if (String(activityMessage.activityId) === String(activityMessage.runningActivityList) && activityMessage.activityStatus === 2) {
            this.parent.emit('currentActivityChanged', this, hubKey);
            this.parent.parent.emit('activityChanged', this.label, hubKey);
            this._emitFixitStates();
        }

        if (String(activityMessage.activityId) !== String(activityMessage.runningActivityList) && activityMessage.activityStatus === 1)
            this.parent.parent.emit('activityChanging', this.label, hubKey);

    }

    _emitFixitStates() {
        if (!this.fixit)
            return;

        for (const propertyName in this.fixit) {
            const deviceState = this.fixit[propertyName];
            if (!deviceState || typeof deviceState !== 'object')
                continue;
            this.parent.emit(`deviceStateChanged_${String(deviceState.id)}`, deviceState);
        }
    }

}

module.exports = HubActivity
