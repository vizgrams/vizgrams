import { test } from '@playwright/test'

const BASE = 'http://localhost:5174'

test('graph debug', async ({ page }) => {
  const logs: string[] = []
  page.on('console', m => logs.push(`[${m.type()}] ${m.text()}`))
  page.on('pageerror', e => logs.push(`[ERROR] ${e.message}`))

  await page.goto(BASE)
  await page.evaluate((key) => localStorage.setItem('explore_model', key), 'example')
  await page.goto(`${BASE}/graph`, { waitUntil: 'networkidle' })
  await page.waitForTimeout(10000)

  const canvasCount = await page.locator('canvas').count()
  const nodeCount = await page.evaluate(() =>
    document.querySelector('p')?.textContent
  )

  // Check canvas pixel content
  const pixels = await page.evaluate(() => {
    const canvases = Array.from(document.querySelectorAll('canvas'))
    return canvases.map(c => {
      const ctx = c.getContext('2d')
      if (!ctx) return 0
      const d = ctx.getImageData(0, 0, c.width, c.height).data
      let count = 0
      for (let i = 0; i < d.length; i += 4) {
        if (d[i] > 20 || d[i+1] > 20 || d[i+2] > 20) count++
      }
      return count
    })
  })

  await page.screenshot({ path: 'graph-debug.png' })

  console.log('Canvas count:', canvasCount)
  console.log('Node text:', nodeCount)
  console.log('Pixels per canvas:', pixels)
  console.log('Console messages:', logs.filter(l => !l.includes('DevTools') && !l.includes('connecting')).join('\n'))
})
