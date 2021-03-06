//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.MetricNameEncodedFactory;
import com.microsoft.kafkaavailability.reporters.ScheduledReporterCollector;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.properties.ProducerProperties;
import kafka.javaapi.TopicMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;

import static com.microsoft.kafkaavailability.discovery.Constants.DEFAULT_ELAPSED_TIME;

public class AvailabilityThread implements Callable<Long> {

    final static Logger m_logger = LoggerFactory.getLogger(AvailabilityThread.class);

    private final ScheduledReporterCollector reporterCollector;
    private final CuratorFramework m_curatorFramework;
    private final AppProperties appProperties;
    private final MetricNameEncodedFactory metricNameFactory;

    private Phaser m_phaser;
    private long m_threadSleepTime;

    @Inject
    public AvailabilityThread(CuratorFramework curatorFramework, ScheduledReporterCollector reporterCollector,
                              AppProperties appProperties,MetricNameEncodedFactory metricNameFactory,
                              @Assisted Phaser phaser, @Assisted long threadSleepTime) {
        this.m_curatorFramework = curatorFramework;
        this.reporterCollector = reporterCollector;
        this.reporterCollector.start();
        this.appProperties = appProperties;
        this.metricNameFactory = metricNameFactory;

        this.m_phaser = phaser;
        //this.m_phaser.register(); //Registers/Add a new unArrived party to this phaser.
        //CommonUtils.dumpPhaserState("After register", phaser);
        this.m_threadSleepTime = threadSleepTime;
    }

    @Override
    public Long call() throws Exception {
        int sleepDuration = 1000;
        long elapsedTime = 0L;
        do {
            long lStartTime = System.currentTimeMillis();
            MetricRegistry metrics;
            m_logger.info(Thread.currentThread().getName() +
                    " - Availability party has arrived and is working in "
                    + "Phase-" + m_phaser.getPhase());

            try {
                metrics = reporterCollector.getRegistry();
                runAvailability(metrics);

            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
            } finally {
                try {
                    reporterCollector.report();
                    CommonUtils.sleep(1000);
                } catch (Exception e) {
                    m_logger.error(e.getMessage(), e);
                }
            }

            elapsedTime = CommonUtils.stopWatch(lStartTime);
            m_logger.info("Availability Elapsed: " + elapsedTime + " milliseconds.");

            while (elapsedTime < m_threadSleepTime && !m_phaser.isTerminated()) {
                try {
                    Thread.currentThread().sleep(sleepDuration);
                    elapsedTime = elapsedTime + sleepDuration;
                } catch (InterruptedException ie) {
                    m_logger.error(ie.getMessage(), ie);
                }
            }
        } while (!m_phaser.isTerminated());

        reporterCollector.stop();
        m_logger.info("AvailabilityThread (run()) has been COMPLETED.");
        return Long.valueOf(elapsedTime);
    }

    private void runAvailability(MetricRegistry metrics) throws IOException, MetaDataManagerException {

        m_logger.info("Starting AvailabilityLatency");

        IPropertiesManager producerPropertiesManager = new PropertiesManager<ProducerProperties>("producerProperties.json", ProducerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);
        MetaDataManagerProperties metaDataProperties = (MetaDataManagerProperties) metaDataPropertiesManager.getProperties();

        IProducer producer = new Producer(producerPropertiesManager, metaDataManager);

        //This is full list of topics
        List<TopicMetadata> totalTopicMetadata = metaDataManager.getAllTopicPartition();

        List<kafka.javaapi.TopicMetadata> whiteListTopicMetadata = new ArrayList<TopicMetadata>();

        for (kafka.javaapi.TopicMetadata topic : totalTopicMetadata) {
            for (String whiteListTopic : metaDataProperties.canaryTestTopics)
                // java string compare while ignoring case
                if (topic.topic().equalsIgnoreCase(whiteListTopic)) {
                    whiteListTopicMetadata.add(topic);
                }
        }

        List<String> gtmList = new ArrayList<String>();
        if (!appProperties.kafkaGTMIP.isEmpty()) {
            gtmList.addAll(appProperties.kafkaGTMIP);
        }

        List<String> vipList = new ArrayList<String>();
        if (!appProperties.kafkaIP.isEmpty()) {
            vipList.addAll(appProperties.kafkaIP);
        }

        postData("KafkaGTMIP", metrics, producer, whiteListTopicMetadata, gtmList,
                appProperties.reportKafkaGTMAvailability, appProperties.sendGTMAvailabilityLatency,
                appProperties.useCertificateToConnectToKafkaGTM, appProperties.keyStoreFilePath,
                appProperties.keyStoreFilePassword);

        postData("KafkaIP", metrics, producer, whiteListTopicMetadata, vipList,
                appProperties.reportKafkaIPAvailability, appProperties.sendKafkaIPAvailabilityLatency,
                appProperties.useCertificateToConnectToKafkaIP, appProperties.keyStoreFilePath,
                appProperties.keyStoreFilePassword);

        ((MetaDataManager) metaDataManager).close();
        m_logger.info("Finished AvailabilityLatency");
    }

    private void postData(String name, MetricRegistry metrics, IProducer producer,
                          List<kafka.javaapi.TopicMetadata> whiteListTopicMetadata, List<String> gtmList,
                          boolean reportAvailability, boolean reportLatency, boolean useCertificateToConnect,
                          String keyStoreFilePath, String keyStoreFilePassword) {

        int numMessages = 100;
        long startTime, endTime;
        int failureThreshold = 10;

        int windowSize = numMessages * ((whiteListTopicMetadata.size() > 0) ? (whiteListTopicMetadata.size()) : 1);

        m_logger.info("Starting " + name + " prop check." + reportAvailability);

        for (String gtm : gtmList) {

            int gtmIPStatusTryCount = 0;
            int gtmIPStatusFailCount = 0;
            String authority = null;

            try {
                URL url = new URL(gtm);
                authority = url.getAuthority();
            } catch (MalformedURLException e) {
                authority = gtm;
            }

            final SlidingWindowReservoir gtmAvailabilityLatencyWindow = new SlidingWindowReservoir(windowSize);
            Histogram histogramGTMAvailabilityLatency = new Histogram(gtmAvailabilityLatencyWindow);
            MetricNameEncoded gtmAvailabilityLatency = metricNameFactory.createWithVIP(name + ".Availability.Latency", authority);
            if (!metrics.getNames().contains(new Gson().toJson(gtmAvailabilityLatency))) {
                if (reportLatency && !gtmList.isEmpty())
                    metrics.register(new Gson().toJson(gtmAvailabilityLatency), histogramGTMAvailabilityLatency);
            }

            for (kafka.javaapi.TopicMetadata item : whiteListTopicMetadata) {
                m_logger.info("Posting to Topic: {} using : {};", item.topic(), gtm);
                int tryCount = 0, failCount = 0;
                for (int i = 0; i < numMessages; i++) {
                    if (reportAvailability) {
                        startTime = System.currentTimeMillis();
                        try {
                            tryCount++;
                            producer.sendCanaryToKafkaIP(gtm, item.topic(), useCertificateToConnect, keyStoreFilePath, keyStoreFilePassword);
                            endTime = System.currentTimeMillis();
                        } catch (Exception e) {
                            failCount++;
                            m_logger.error(name + " -- Error Writing to Topic: {} using : {}; Exception: {}", item.topic(), gtm, e);
                            endTime = System.currentTimeMillis() + DEFAULT_ELAPSED_TIME;
                        }
                        histogramGTMAvailabilityLatency.update(endTime - startTime);
                    }
                    if (failCount >= 10) {
                        m_logger.error(name + ": {} has failed more than {} times. Giving up!!!.", gtm, failureThreshold);
                        tryCount = failCount = 100;
                        break;
                    }
                }
                gtmIPStatusTryCount = gtmIPStatusTryCount + tryCount;
                gtmIPStatusFailCount = gtmIPStatusFailCount + failCount;
            }
            if (reportAvailability && !gtmList.isEmpty()) {
                m_logger.info("About to report " + name + "Availability-- TryCount:" + gtmIPStatusTryCount + " FailCount:" + gtmIPStatusFailCount);
                MetricNameEncoded kafkaGTMIPAvailability = metricNameFactory.createWithVIP(name + ".Availability", authority);
                if (!metrics.getNames().contains(new Gson().toJson(kafkaGTMIPAvailability))) {
                    metrics.register(new Gson().toJson(kafkaGTMIPAvailability), new AvailabilityGauge(gtmIPStatusTryCount, gtmIPStatusTryCount - gtmIPStatusFailCount));
                }
            }
        }
    }
}