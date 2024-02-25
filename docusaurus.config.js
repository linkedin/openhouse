// @ts-check
// `@type` JSDoc annotations allow editor autocompletion and type checking
// (when paired with `@ts-check`).
// There are various equivalent ways to declare your Docusaurus config.
// See: https://docusaurus.io/docs/api/docusaurus-config

import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'OpenHouse',
  tagline: 'Control Plane for Tables in Open Data Lakehouses',
  favicon: 'ui/openhouse_logo_without_text.png',

  // Set the production url of your site here
  url: 'https://www.openhousedb.org',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'linkedin', // Usually your GitHub org/user name.
  projectName: 'openhouse', // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  customFields: {
    oneLineSummary: 'OpenHouse empowers Lakehouse users to self-serve creation of Managed, Shareable & Compliant Tables.',
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js'
        },
        blog: {
          showReadingTime: true,
          readingTime: ({content, frontMatter, defaultReadingTime}) =>
            frontMatter.hide_reading_time
              ? undefined
              : defaultReadingTime({content})
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],[
      'redocusaurus',
      {
        specs: [
          {
            id: 'tables-openapi-spec-json',
            spec: 'specs/tables.json'
          },
          {
            id: 'jobs-openapi-spec-json',
            spec: 'specs/jobs.json'
          }
        ],
      },
    ]
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'ui/docusaurus-social-card.jpg',
      navbar: {
        title: 'OpenHouse',
        logo: {
          alt: 'OpenHouse Logo',
          src: 'ui/openhouse_logo_without_text.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'right',
            label: 'Docs',
          },
          {to: '/blog', label: 'Blog', position: 'right'},
          {
            href: 'https://github.com/linkedin/openhouse',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Overview',
                to: '/docs/intro',
              },
              {
                label: 'User Guide',
                to: '/docs/category/user-guide',
              }
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'LinkedIn Eng Blog',
                href: 'https://www.linkedin.com/blog/engineering/data-management/taking-charge-of-tables--introducing-openhouse-for-big-data-mana',
              },
              {
                label: 'Slack',
                href: 'https://join.slack.com/t/openhouse-bap9266/shared_invite/zt-2clj3hb3l-NMb4UaImftJ4Xlj3oBQDrg',
              }
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/linkedin/openhouse',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} LinkedIn Corporation`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    }),
};

export default config;
