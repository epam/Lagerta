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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Evgeniy_Ignatiev
 * @since 1/18/2017 3:28 PM
 */
public class RetryStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryStrategy.class);

    private final List<Class<? extends Exception>> retriableExceptions = new ArrayList<>();

    private long backoffMultiplier;
    private long delay;
    private int retries;
    private Runnable onStop;
    private Runnable onFailure;

    private int currentRetry = 0;

    public RetryStrategy backoff(long backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
        return this;
    }

    public RetryStrategy delay(long delay) {
        this.delay = delay;
        return this;
    }

    public RetryStrategy retries(int retries) {
        this.retries = retries;
        return this;
    }

    public RetryStrategy retryException(Class<? extends Exception> clazz) {
        retriableExceptions.add(clazz);
        return this;
    }

    public RetryStrategy retryExceptions(List<Class<? extends Exception>> classes) {
        retriableExceptions.addAll(classes);
        return this;
    }

    public RetryStrategy onFailure(Runnable onFailure) {
        this.onFailure = onFailure;
        return this;
    }

    public RetryStrategy onStop(Runnable onStop) {
        this.onStop = onStop;
        return this;
    }

    public boolean isRetryPossible(Exception exception) {
        LOGGER.info("[G] Caught exception while retrying", exception);
        if (onFailure != null) {
            onFailure.run();
        }
        if (currentRetry < retries && isRetriableException(exception)) {
            try {
                long sleepTime = delay * (++currentRetry * backoffMultiplier);
                LOGGER.debug("[G] Sleeping for {}", sleepTime);
                Thread.sleep(sleepTime);
                return true;
            } catch (InterruptedException e) {
                // no-op
            }
        }
        LOGGER.debug("[G] Stopping processing retries");
        if (onStop != null) {
            onStop.run();
        }
        return false;
    }

    private boolean isRetriableException(Exception exception) {
        if (retriableExceptions.isEmpty()) {
            return true;
        }
        for (Class<? extends Exception> clazz : retriableExceptions) {
            if (clazz.isAssignableFrom(exception.getClass())) {
                return true;
            }
        }
        return false;
    }
}
