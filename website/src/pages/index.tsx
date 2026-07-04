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
    description: 'Per-agent access control with default-deny policies, rate limiting, TLS 1.3, and a full audit trail of every interaction.',
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
    description: 'Prometheus metrics, OpenTelemetry tracing, Grafana dashboards, and a built-in web UI for real-time monitoring.',
  },
];

const useCases = [
  {
    prompt: '"List all Kafka topics and their partition counts"',
    tool: 'kafka_list_topics',
    description: 'Discover your entire Kafka topology',
  },
  {
    prompt: '"Show me the last 10 messages from orders.created"',
    tool: 'kafka_consume',
    description: 'Read messages with flexible positioning',
  },
  {
    prompt: '"What\'s the lag on the order-processor consumer group?"',
    tool: 'kafka_describe_consumer_group',
    description: 'Monitor consumer health and lag',
  },
  {
    prompt: '"Analyze the dead-letter queue for error patterns"',
    tool: 'kafka_analyze_dlq',
    description: 'Categorize and diagnose DLQ errors',
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
          <img src="https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go&logoColor=white" alt="Go" />
          <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License" />
          <img src="https://img.shields.io/badge/MCP-compatible-8A2BE2" alt="MCP" />
          <img src="https://img.shields.io/badge/Default%20Surface-%E2%89%A415%20tools-green" alt="Default ≤15 tools" />
        </div>
        <div className={styles.installCommand}>
          <code>go install github.com/josedab/kafkamcp/cmd/kafkamcp@latest</code>
        </div>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/getting-started">
            Get Started in 5 Minutes →
          </Link>
          <Link className="button button--outline button--lg" to="https://github.com/josedab/kafkamcp" style={{marginLeft: '1rem', color: 'white', borderColor: 'white'}}>
            ★ GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

function HowItWorksSection() {
  return (
    <section className={styles.howItWorks}>
      <div className="container">
        <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem'}}>How It Works</Heading>
        <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem', maxWidth: '600px', margin: '0 auto 2rem'}}>
          Three steps to give your AI agents full Kafka access.
        </p>
        <div className="row">
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>1</div>
              <Heading as="h3">Install</Heading>
              <code className={styles.stepCode}>go install github.com/josedab/kafkamcp/cmd/kafkamcp@latest</code>
              <p>Single Go binary, no JVM or CGO. Signed Docker images and pre-built binaries begin with the first tagged release.</p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>2</div>
              <Heading as="h3">Configure</Heading>
              <code className={styles.stepCode}>kafkamcp init --output kafkamcp.yaml</code>
              <p>Point it at your Kafka clusters. Add Schema Registry, access policies, and audit logging as needed.</p>
            </div>
          </div>
          <div className="col col--4">
            <div className={styles.stepCard}>
              <div className={styles.stepNumber}>3</div>
              <Heading as="h3">Connect</Heading>
              <code className={styles.stepCode}>kafkamcp --config kafkamcp.yaml</code>
              <p>Wire it into Claude Desktop, LangChain, CrewAI, or any MCP-compatible client. Agents get instant Kafka access.</p>
            </div>
          </div>
        </div>
      </div>
    </section>
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
        <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem'}}>Why KafkaMCP</Heading>
        <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem'}}>
          Everything agents need to work with Kafka — built into a single server.
        </p>
        <div className="row">
          {features.map((f, idx) => (
            <FeatureCard key={idx} title={f.title} description={f.description} />
          ))}
        </div>
      </div>
    </section>
  );
}

function UseCasesSection() {
  return (
    <section className={styles.codeSection}>
      <div className="container">
        <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem'}}>Your Agent, Connected to Kafka</Heading>
        <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem'}}>
          Once KafkaMCP is running, agents interact with Kafka using natural language.
        </p>
        <div className="row">
          <div className="col col--6">
            <div className={styles.useCaseList}>
              {useCases.map((uc, idx) => (
                <div key={idx} className={styles.useCaseItem}>
                  <div className={styles.useCasePrompt}>{uc.prompt}</div>
                  <div className={styles.useCaseMeta}>
                    <code>{uc.tool}</code> — {uc.description}
                  </div>
                </div>
              ))}
            </div>
            <p style={{marginTop: '1.5rem'}}>
              <strong>Read-only by default</strong> (≤15 tools in <code>core-read</code> pack).
              Additional packs — <code>ops-write</code> and <code>governance</code> —
              unlock real, guarded mutations and policy controls behind approval enforcement.
              Full tool catalog and <strong>9 MCP resources</strong> cover discovery, consumption, schema
              management, consumer groups, connectors, and more.
            </p>
          </div>
          <div className="col col--6">
            <div className={styles.configBlock}>
              <div className={styles.configHeader}>kafkamcp.yaml</div>
              <pre className={styles.configPre}>{`server:
  transport: stdio

clusters:
  - name: production
    bootstrap_servers: "broker:9092"
    default: true
    schema_registry:
      url: "http://schema-registry:8081"

policies:
  default_deny: true
  agents:
    - id: "my-agent"
      topics:
        - pattern: "orders.*"
          permissions: [read, write]
      rate_limit:
        requests_per_minute: 120

audit:
  enabled: true`}</pre>
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
        <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem'}}>Architecture</Heading>
        <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem'}}>
          A purpose-built bridge between AI agents and Apache Kafka.
        </p>
        <div style={{textAlign: 'center'}}>
          <pre className={styles.architecturePre}>
{`┌──────────────────────────────────────────────────────────┐
│  AI Agents (Claude, LangChain, CrewAI, AutoGen, Custom)  │
└──────────────┬─────────────────────┬─────────────────────┘
               │ stdio               │ SSE / HTTP
┌──────────────▼─────────────────────▼─────────────────────┐
│                   KafkaMCP Server                         │
│  ┌─────────┐ ┌─────────┐ ┌──────┐ ┌────────┐ ┌────────┐ │
│  │ ≤15     │ │ 18      │ │ Auth │ │ Rate   │ │ Audit  │ │
│  │ Tools * │ │ Rsrcs   │ │Engine│ │Limiter │ │ Logger │ │
│  └────┬────┘ └────┬────┘ └──────┘ └────────┘ └────────┘ │
│       └───────────┼──────────────────────────────────────│
│  ┌────────────────▼────────────────────────────────────┐ │
│  │  Kafka Client (franz-go) │ Schema Registry │ Connect│ │
│  └────────────────┬────────────────────────────────────┘ │
└───────────────────┼──────────────────────────────────────┘
     ┌──────────────┼──────────────┐
     ▼              ▼              ▼
┌─────────┐  ┌───────────┐  ┌──────────┐
│  Kafka  │  │  Schema   │  │  Kafka   │
│ Brokers │  │ Registry  │  │ Connect  │
└─────────┘  └───────────┘  └──────────┘
* Default core-read pack. ops-write/governance packs available.`}
          </pre>
        </div>
        <div style={{textAlign: 'center', marginTop: '1.5rem'}}>
          <Link className="button button--primary button--md" to="/docs/architecture">
            Read the Architecture Guide →
          </Link>
        </div>
      </div>
    </section>
  );
}

function CommunitySection() {
  return (
    <section className={styles.communitySection}>
      <div className="container">
        <Heading as="h2" style={{textAlign: 'center', marginBottom: '0.5rem'}}>Join the Community</Heading>
        <p style={{textAlign: 'center', color: 'var(--ifm-color-emphasis-700)', marginBottom: '2rem'}}>
          KafkaMCP is open source under the Apache 2.0 license. We welcome contributions of all kinds.
        </p>
        <div className="row" style={{justifyContent: 'center'}}>
          <div className="col col--3" style={{textAlign: 'center'}}>
            <Link to="https://github.com/josedab/kafkamcp" className={styles.communityLink}>
              <div className={styles.communityIcon}>🐙</div>
              <strong>GitHub</strong>
              <p>Star, fork, contribute</p>
            </Link>
          </div>
          <div className="col col--3" style={{textAlign: 'center'}}>
            <Link to="https://github.com/josedab/kafkamcp/discussions" className={styles.communityLink}>
              <div className={styles.communityIcon}>💬</div>
              <strong>Discussions</strong>
              <p>Ask questions, share ideas</p>
            </Link>
          </div>
          <div className="col col--3" style={{textAlign: 'center'}}>
            <Link to="https://github.com/josedab/kafkamcp/issues/new?template=design_partner_feedback.yml" className={styles.communityLink}>
              <div className={styles.communityIcon}>🤝</div>
              <strong>Design Partner</strong>
              <p>Share how you use KafkaMCP</p>
            </Link>
          </div>
          <div className="col col--3" style={{textAlign: 'center'}}>
            <Link to="/docs/contributing" className={styles.communityLink}>
              <div className={styles.communityIcon}>📝</div>
              <strong>Contribute</strong>
              <p>Read the contributor guide</p>
            </Link>
          </div>
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
        <HowItWorksSection />
        <FeaturesSection />
        <UseCasesSection />
        <ArchitectureSection />
        <CommunitySection />
      </main>
    </Layout>
  );
}
