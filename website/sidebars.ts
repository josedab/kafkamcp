import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    'getting-started',
    'core-concepts',
    'architecture',
    'security',
    {
      type: 'category',
      label: 'Guides',
      collapsed: false,
      items: [
        'guides/claude-desktop',
        'guides/docker-compose',
        'guides/multi-cluster',
        'guides/access-control',
        'guides/monitoring',
        'guides/dashboard',
        'guides/openapi-spec',
        'guides/intellij',
      ],
    },
    {
      type: 'category',
      label: 'SDKs & Integrations',
      collapsed: false,
      items: [
        'guides/typescript-sdk',
        'guides/go-sdk',
        'guides/python-sdk',
        'guides/langchain-sdk',
        'guides/crewai-sdk',
      ],
    },
    {
      type: 'category',
      label: 'API Reference',
      collapsed: false,
      items: [
        'api-reference/tools',
        'api-reference/resources',
      ],
    },
    'cli-reference',
    'configuration',
    {
      type: 'category',
      label: 'Deployment',
      items: [
        'deployment/binary',
        'deployment/docker',
        'deployment/kubernetes',
      ],
    },
    'comparison',
    'troubleshooting',
    'faq',
    'contributing',
  ],
};

export default sidebars;
