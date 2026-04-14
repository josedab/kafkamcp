import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

const features = [
  {
    title: '🔍 Discover',
    description: 'Agents list topics, browse schemas, inspect consumer groups, and explore cluster topology — with zero prior knowledge.',
  },
  {
    title: '📨 Consume & Produce',
    description: 'Read messages with flexible positioning (offset, timestamp, latest) and publish with delivery confirmation and schema validation.',
  },
  {
    title: '🔒 Secure by Default',
    description: 'Per-agent access control with default-deny policies, rate limiting, TLS, and a full audit trail of every interaction.',
  },
  {
    title: '📋 Schema-Aware',
    description: 'Auto-discovers Avro, Protobuf, and JSON schemas from Schema Registry. Agents understand message structure without human annotation.',
  },
  {
    title: '🏗️ Multi-Cluster',
    description: 'Connect to multiple Kafka clusters. Each tool accepts a cluster parameter. Agents discover clusters via MCP resources.',
  },
  {
    title: '📊 Observable',
    description: 'Prometheus metrics for every tool call — latency histograms, throughput counters, auth denials, and rate limit tracking.',
  },
];

function HeroSection() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.badgeRow}>
          <img src="https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white" alt="Go" />
          <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License" />
          <img src="https://img.shields.io/badge/MCP-compatible-8A2BE2" alt="MCP" />
          <img src="https://img.shields.io/badge/Kafka-compatible-231F20?logo=apachekafka&logoColor=white" alt="Kafka" />
        </div>
        <div className={styles.installCommand}>
          <code>go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest</code>
        </div>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/getting-started">
            Get Started in 5 Minutes →
          </Link>
          <Link className="button button--outline button--lg" to="https://github.com/kafkamcp/kafkamcp" style={{marginLeft: '1rem', color: 'white', borderColor: 'white'}}>
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

function FeatureCard({title, description}: {title: string; description: string}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="feature-card" style={{height: '100%', marginBottom: '1rem'}}>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

function FeaturesSection() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {features.map((f, idx) => (
            <FeatureCard key={idx} title={f.title} description={f.description} />
          ))}
        </div>
      </div>
    </section>
  );
}

function CodeExample() {
  return (
    <section className={styles.codeSection}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <Heading as="h2">Your agent, connected to Kafka</Heading>
            <p>Once KafkaMCP is running, your AI agent can interact with Kafka using natural language:</p>
            <ul>
              <li><em>"List all Kafka topics in my cluster"</em></li>
              <li><em>"Show me the last 10 messages from orders.created"</em></li>
              <li><em>"Describe the consumer group order-processor"</em></li>
              <li><em>"Check if this schema change is compatible"</em></li>
              <li><em>"Analyze the dead-letter queue for errors"</em></li>
            </ul>
            <p>22 MCP tools and 10 resources — covering discovery, consumption, production, schema management, consumer groups, topic administration, stream processing, and more.</p>
          </div>
          <div className="col col--6">
            <div style={{background: 'var(--ifm-color-emphasis-100)', borderRadius: '12px', padding: '1.5rem', fontFamily: 'var(--ifm-font-family-monospace)', fontSize: '0.85rem', lineHeight: 1.6}}>
              <div style={{color: 'var(--ifm-color-emphasis-600)', marginBottom: '0.5rem'}}>{'# kafkamcp.yaml'}</div>
              <div><span style={{color: 'var(--ifm-color-primary)'}}>server</span>:</div>
              <div>&nbsp;&nbsp;<span style={{color: 'var(--ifm-color-primary)'}}>transport</span>: stdio</div>
              <div style={{marginTop: '0.5rem'}}><span style={{color: 'var(--ifm-color-primary)'}}>clusters</span>:</div>
              <div>&nbsp;&nbsp;- <span style={{color: 'var(--ifm-color-primary)'}}>name</span>: production</div>
              <div>&nbsp;&nbsp;&nbsp;&nbsp;<span style={{color: 'var(--ifm-color-primary)'}}>bootstrap_servers</span>: "broker:9092"</div>
              <div>&nbsp;&nbsp;&nbsp;&nbsp;<span style={{color: 'var(--ifm-color-primary)'}}>default</span>: true</div>
              <div style={{marginTop: '0.5rem'}}><span style={{color: 'var(--ifm-color-primary)'}}>audit</span>:</div>
              <div>&nbsp;&nbsp;<span style={{color: 'var(--ifm-color-primary)'}}>enabled</span>: true</div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function ArchitectureSection() {
  return (
    <section className={styles.architectureSection}>
      <div className="container">
        <Heading as="h2" style={{textAlign: 'center', marginBottom: '2rem'}}>Architecture</Heading>
        <div style={{textAlign: 'center'}}>
          <pre style={{display: 'inline-block', textAlign: 'left', fontSize: '0.8rem', lineHeight: 1.5, background: 'var(--ifm-color-emphasis-100)', padding: '1.5rem', borderRadius: '12px', maxWidth: '100%', overflow: 'auto'}}>
{`┌──────────────────────────────────────────────────────┐
│  AI Agents (Claude, LangChain, AutoGen, CrewAI)      │
└──────────────┬─────────────────────┬─────────────────┘
               │ stdio               │ SSE/HTTP
┌──────────────▼─────────────────────▼─────────────────┐
│              KafkaMCP Server                          │
│  ┌────────┐ ┌────────┐ ┌──────┐ ┌────────┐ ┌──────┐ │
│  │ 22     │ │ 10     │ │ Auth │ │ Rate   │ │Audit │ │
│  │ Tools  │ │ Rsrcs  │ │Engine│ │Limiter │ │ Log  │ │
│  └────┬───┘ └────┬───┘ └──────┘ └────────┘ └──────┘ │
│       └──────────┼───────────────────────────────────│
│  ┌───────────────▼───────────────────────────────┐   │
│  │ Kafka Client (franz-go) │ Schema Registry     │   │
│  │ Admin │ Consumer │ Producer │ HTTP Client      │   │
│  └───────────────┬───────────────────────────────┘   │
└──────────────────┼───────────────────────────────────┘
     ┌─────────────┼─────────────┐
     ▼             ▼             ▼
┌─────────┐ ┌───────────┐ ┌──────────┐
│  Kafka  │ │  Schema   │ │  Kafka   │
│ Brokers │ │ Registry  │ │ Connect  │
└─────────┘ └───────────┘ └──────────┘`}
          </pre>
        </div>
      </div>
    </section>
  );
}

export default function Home(): React.JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title="Home" description={siteConfig.tagline}>
      <HeroSection />
      <main>
        <FeaturesSection />
        <CodeExample />
        <ArchitectureSection />
      </main>
    </Layout>
  );
}
