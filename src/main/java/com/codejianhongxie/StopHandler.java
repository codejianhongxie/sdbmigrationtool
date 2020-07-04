package com.codejianhongxie;

import com.codejianhongxie.executor.BaseThread;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class StopHandler implements SignalHandler {
	private BaseThread thread;

	public StopHandler(BaseThread executor) {
		this.thread = executor;
	}

	public void registerSignal(String signalName) {
		Signal signal = new Signal(signalName);
		Signal.handle(signal, this);
	}

	@Override
	public void handle(Signal signal) {
		thread.stop();
	}
}