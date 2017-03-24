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

package org.apache.ignite.activestore.commons.retry;

/**
 * @author Andrei_Yakushin
 * @since 12/16/2016 9:43 AM
 */
public class RetryRunnable implements Runnable {
    private final Runnable inner;
    private final RetryStrategy retryStrategy;

    public RetryRunnable(Runnable inner, RetryStrategy retryStrategy) {
        this.inner = inner;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void run() {
        Exception exception;
        do {
            try {
                inner.run();
                return;
            } catch (Exception e) {
                exception = e;
            }
        } while (retryStrategy.isRetryPossible(exception));
    }
}
