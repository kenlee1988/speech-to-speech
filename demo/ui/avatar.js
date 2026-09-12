// @ts-check

/** Wait for non-trickle ICE so the same-origin offer proxy remains stateless. */
function waitForIceGathering(pc, timeoutMs = 5000) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      pc.removeEventListener("icegatheringstatechange", changed);
      resolve();
    };
    const changed = () => {
      if (pc.iceGatheringState === "complete") done();
    };
    const timer = window.setTimeout(done, timeoutMs);
    pc.addEventListener("icegatheringstatechange", changed);
  });
}

function waitForConnected(pc, timeoutMs = 15000) {
  if (pc.connectionState === "connected") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const done = () => {
      clearTimeout(timer);
      pc.removeEventListener("connectionstatechange", changed);
    };
    const changed = () => {
      if (pc.connectionState === "connected") {
        done();
        resolve();
      } else if (["failed", "closed"].includes(pc.connectionState)) {
        done();
        reject(new Error(`Avatar WebRTC ${pc.connectionState}`));
      }
    };
    const timer = window.setTimeout(() => {
      done();
      reject(new Error("Avatar WebRTC connection timed out"));
    }, timeoutMs);
    pc.addEventListener("connectionstatechange", changed);
  });
}

/** LiveTalking receive-only WebRTC view sharing the conversation AudioContext. */
export class AvatarView extends EventTarget {
  /** @param {{panel: HTMLElement, video: HTMLVideoElement, status: HTMLElement}} elements */
  constructor({ panel, video, status }) {
    super();
    this.panel = panel;
    this.video = video;
    this.status = status;
    /** @type {RTCPeerConnection | null} */
    this.pc = null;
    /** @type {MediaStream | null} */
    this.stream = null;
    /** @type {MediaStreamAudioSourceNode | null} */
    this.audioSource = null;
    /** @type {GainNode | null} */
    this.audioGain = null;
    this.intentionalClose = false;
  }

  setState(state, label) {
    this.panel.dataset.state = state;
    this.status.textContent = label;
  }

  /**
   * @param {AudioContext} audioContext
   * @param {string} [offerUrl]
   * @param {RTCIceServer[]} [iceServers]
   */
  async connect(audioContext, offerUrl = "/api/avatar/offer", iceServers = []) {
    await this.close();
    this.intentionalClose = false;
    this.panel.hidden = false;
    this.setState("connecting", "Connecting avatar…");

    const pc = new RTCPeerConnection({ iceServers });
    const stream = new MediaStream();
    this.pc = pc;
    this.stream = stream;
    this.video.muted = true; // audio is routed only through the graph below
    this.video.srcObject = stream;

    pc.addTransceiver("video", { direction: "recvonly" });
    pc.addTransceiver("audio", { direction: "recvonly" });
    pc.addEventListener("track", (event) => {
      stream.addTrack(event.track);
      if (event.track.kind === "audio" && !this.audioSource) {
        const audioOnly = new MediaStream([event.track]);
        this.audioSource = audioContext.createMediaStreamSource(audioOnly);
        this.audioGain = audioContext.createGain();
        this.audioSource.connect(this.audioGain);
        this.audioGain.connect(audioContext.destination);
      }
      void this.video.play().catch(() => {});
    });
    pc.addEventListener("connectionstatechange", () => {
      if (!this.intentionalClose && ["failed", "disconnected", "closed"].includes(pc.connectionState)) {
        this.setState("error", "Avatar disconnected — using voice audio");
        this.dispatchEvent(new Event("disconnected"));
        this._release(true);
      }
    });

    try {
      await pc.setLocalDescription(await pc.createOffer());
      await waitForIceGathering(pc);
      const response = await fetch(offerUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sdp: pc.localDescription?.sdp,
          type: pc.localDescription?.type,
        }),
      });
      if (!response.ok) {
        let detail = `${response.status}`;
        try { detail = (await response.json()).detail || detail; } catch {}
        throw new Error(`Avatar offer failed (${detail})`);
      }
      const answer = await response.json();
      if (!answer.sdp || !answer.type) throw new Error("Avatar returned an invalid answer");
      await pc.setRemoteDescription(answer);
      await waitForConnected(pc);
      this.setState("connected", "Avatar connected");
      return true;
    } catch (error) {
      this.setState("error", "Avatar unavailable — using voice audio");
      this._release(true);
      throw error;
    }
  }

  _release(intentional) {
    this.intentionalClose = intentional;
    const pc = this.pc;
    this.pc = null;
    try { pc?.close(); } catch {}
    try { this.audioSource?.disconnect(); } catch {}
    try { this.audioGain?.disconnect(); } catch {}
    this.audioSource = null;
    this.audioGain = null;
    for (const track of this.stream?.getTracks() ?? []) track.stop();
    this.stream = null;
    this.video.srcObject = null;
  }

  async close() {
    this._release(true);
    this.setState("idle", "Avatar idle");
  }
}
