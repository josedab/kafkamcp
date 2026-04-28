import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    'getting-started',
    'core-concepts',
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
        'guides/typescript-sdk',
        'guides/openapi-spec',
        'guides/intellij',
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
    'contributing',
  ],
};

export default sidebars;
