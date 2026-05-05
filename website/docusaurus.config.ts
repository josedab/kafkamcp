import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'KafkaMCP',
  tagline: 'Make Kafka topics fully readable, writable, and discoverable by AI agents',
  favicon: 'img/favicon.ico',

  future: {
    v4: true,
  },

  url: 'https://kafkamcp.dev',
  baseUrl: '/',

  organizationName: 'kafkamcp',
  projectName: 'kafkamcp',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['en'],
        indexBlog: false,
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/kafkamcp/kafkamcp/tree/main/website/',
        },
        blog: {
          showReadingTime: true,
          onInlineAuthors: 'ignore',
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          editUrl: 'https://github.com/kafkamcp/kafkamcp/tree/main/website/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/kafkamcp-social-card.png',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'KafkaMCP',
      logo: {
        alt: 'KafkaMCP Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          to: '/docs/api-reference/tools',
          label: 'API',
          position: 'left',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {
          href: 'https://github.com/kafkamcp/kafkamcp',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Getting Started', to: '/docs/getting-started'},
            {label: 'Core Concepts', to: '/docs/core-concepts'},
            {label: 'Architecture', to: '/docs/architecture'},
            {label: 'API Reference', to: '/docs/api-reference/tools'},
            {label: 'Configuration', to: '/docs/configuration'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub Discussions', href: 'https://github.com/kafkamcp/kafkamcp/discussions'},
            {label: 'Discord', href: 'https://discord.gg/kafkamcp'},
            {label: 'Twitter / X', href: 'https://x.com/kafkamcp'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'Blog', to: '/blog'},
            {label: 'FAQ', to: '/docs/faq'},
            {label: 'GitHub', href: 'https://github.com/kafkamcp/kafkamcp'},
            {label: 'Releases', href: 'https://github.com/kafkamcp/kafkamcp/releases'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} KafkaMCP Contributors. Apache-2.0 License.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'yaml', 'json', 'go'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
