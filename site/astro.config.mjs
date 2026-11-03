import starlight from "@astrojs/starlight"
import { defineConfig } from "astro/config"

export default defineConfig({
  site: "https://jpowersdev.github.io",
  base: "/effect-delta",
  integrations: [
    starlight({
      title: "effect-delta",
      description: "Collaborative text for Effect: replicas that merge without conflicts, sync over any transport, and presence.",
      logo: { src: "./src/assets/logo.svg" },
      favicon: "/favicon.svg",
      customCss: ["./src/styles/custom.css"],
      // Code fits the content column from 1440px up; narrower screens wrap it
      expressiveCode: { defaultProps: { wrap: true } },
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/jpowersdev/effect-delta" }],
      editLink: { baseUrl: "https://github.com/jpowersdev/effect-delta/edit/main/site/" },
      sidebar: [
        { label: "Getting started", slug: "getting-started" },
        { label: "Guides", items: [{ autogenerate: { directory: "guides" } }] },
        { label: "API reference", items: [{ autogenerate: { directory: "api" } }] }
      ]
    })
  ]
})
