/*
 * Copyright (c) 2017. EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.load;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/12/2017 6:55 PM
 */
public class WorkerThread {
    private final Worker worker;

    private String name;
    private Thread thread;

    public WorkerThread(final Worker worker, String name, final long startPosition, final long endPosition) {
        this.worker = worker;
        thread = new Thread(new Runnable() {
            @Override public void run() {
                worker.run(startPosition, endPosition);
            }
        });
        thread.setName(name);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void start() {
        thread.start();
    }

    public boolean isAlive() {
        return thread.isAlive();
    }

    public boolean isFailed() {
        return worker.isFailed();
    }

    public void stop() {
        worker.stop();
    }

    public void join(long timeout) throws InterruptedException {
        thread.join(timeout);
    }

    public long getMsgProcessed() {
        return worker.getMsgProcessed();
    }

    public long getErrorsCount() {
        return worker.getErrorsCount();
    }

    public long getSpeed() {
        return worker.getSpeed();
    }
}
