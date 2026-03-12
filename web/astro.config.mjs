// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'distvirt',
			sidebar: [
				{
					label: 'Guides',
					autogenerate: { directory: 'guides' },
				},
				{
					label: 'Architecture',
					items: [
						{ slug: 'architecture/overview' },
						{
							label: 'Core',
							autogenerate: { directory: 'architecture/core' },
						},
						{
							label: 'Networking',
							autogenerate: { directory: 'architecture/networking' },
						},
						{
							label: 'VM Runtime',
							autogenerate: { directory: 'architecture/runtime' },
						},
						{
							label: 'Meta',
							autogenerate: { directory: 'architecture/meta' },
						},
					],
				},
				{
					label: 'Reference',
					autogenerate: { directory: 'reference' },
				},
			],
		}),
	],
});
