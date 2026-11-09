import { ReadableStream } from 'stream/web';
import type { ReadableStreamDefaultController } from 'stream/web';
import { PassThrough } from 'stream';
import { once } from 'events';
import { setImmediate, setTimeout } from 'timers/promises';
import { describe, expect, it, vi } from 'vitest';
import Browser from '../../src/browser/Browser.js';
import * as PropertySymbol from '../../src/PropertySymbol.js';
import FetchBodyUtility from '../../src/fetch/utilities/FetchBodyUtility.js';

const cases = <const>[
	{ method: 'arrayBuffer', contentType: 'text/plain', text: 'hello' },
	{ method: 'blob', contentType: 'text/plain', text: 'hello' },
	{ method: 'buffer', contentType: 'text/plain', text: 'hello' },
	{ method: 'text', contentType: 'text/plain', text: 'hello' },
	{ method: 'json', contentType: 'application/json', text: '{"key":"value"}' },
	{ method: 'formData', contentType: 'application/x-www-form-urlencoded', text: 'key=value' },
	{
		method: 'formData',
		contentType: 'multipart/form-data; boundary=test',
		text: '--test\r\nContent-Disposition: form-data; name="key"\r\n\r\nvalue\r\n--test--\r\n'
	}
];

describe.each(<const>['Request', 'Response'])('%s body reads', (kind) => {
	describe.each(cases)('$method ($contentType)', ({ method, contentType, text }) => {
		it('Keeps waitUntilComplete pending until the real stream finishes.', async () => {
			const browser = new Browser();
			const page = browser.newPage();
			const window = page.mainFrame.window;
			let controller!: ReadableStreamDefaultController;
			const body = new ReadableStream({ start: (value) => (controller = value) });
			const message =
				kind === 'Request'
					? new window.Request('https://example.com', { method: 'POST', body })
					: new window.Response(body, { headers: { 'Content-Type': contentType } });
			if (kind === 'Request') {
				message[PropertySymbol.contentType] = contentType;
			}
			const read = message[method]();
			let complete = false;
			const wait = page.waitUntilComplete().then(() => (complete = true));
			await setImmediate();
			expect(complete).toBe(false);
			controller.enqueue(Buffer.from(text));
			controller.close();
			await read;
			await wait;
			expect(complete).toBe(true);
			expect(body.locked).toBe(false);
			await browser.close();
		});

		it.each(<const>['page', 'browser'])(
			'Rejects a stalled read when closing the %s.',
			async (target) => {
				const browser = new Browser();
				const page = browser.newPage();
				const window = page.mainFrame.window;
				const cancel = vi.fn(() => new Promise<void>(() => {}));
				const body = new ReadableStream({ cancel });
				const message =
					kind === 'Request'
						? new window.Request('https://example.com', { method: 'POST', body })
						: new window.Response(body, { headers: { 'Content-Type': contentType } });
				if (kind === 'Request') {
					message[PropertySymbol.contentType] = contentType;
				}
				const rejected = expect(message[method]()).rejects.toMatchObject({ name: 'AbortError' });
				await (target === 'page' ? page.close() : browser.close());
				await rejected;
				expect(cancel).toHaveBeenCalledWith(expect.objectContaining({ name: 'AbortError' }));
				expect(body.locked).toBe(false);
				await browser.close();
			}
		);

		it('Reads buffered bodies after close and still enforces bodyUsed.', async () => {
			const browser = new Browser();
			const window = browser.newPage().mainFrame.window;
			const message =
				kind === 'Request'
					? new window.Request('https://example.com', { method: 'POST', body: text })
					: new window.Response(text, { headers: { 'Content-Type': contentType } });
			if (kind === 'Request') {
				message[PropertySymbol.contentType] = contentType;
			}
			await browser.close();
			const result = await message[method]();
			if (method === 'formData') {
				expect(result.get('key')).toBe('value');
			} else if (method === 'json') {
				expect(result).toEqual({ key: 'value' });
			} else if (method === 'text') {
				expect(result).toBe(text);
			} else if (method === 'blob') {
				expect(await result.text()).toBe(text);
			} else {
				expect(Buffer.from(result).toString()).toBe(text);
			}
			await expect(message[method]()).rejects.toMatchObject({ name: 'InvalidStateError' });
		});
	});
});

describe('Body read teardown', () => {
	it('Destroys a pending Node.js source without leaving late data handlers.', async () => {
		const browser = new Browser();
		const page = browser.newPage();
		const nodeStream = new PassThrough();
		const response = new page.mainFrame.window.Response(
			FetchBodyUtility.nodeToWebStream(nodeStream)
		);
		const rejected = expect(response.text()).rejects.toMatchObject({ name: 'AbortError' });
		nodeStream.write(Buffer.from('partial'));
		await setImmediate();
		await page.close();
		await rejected;
		expect(nodeStream.destroyed).toBe(true);
		expect(nodeStream.listenerCount('data')).toBe(0);
		expect(nodeStream.listenerCount('end')).toBe(0);
		await browser.close();
	});

	it('Aborts a read waiting for another chunk and ignores a rejected cancel promise.', async () => {
		const browser = new Browser();
		const page = browser.newPage();
		const body = new ReadableStream({
			start(controller) {
				controller.enqueue(Buffer.from('partial'));
			},
			cancel() {
				return Promise.reject(new Error('cancel failed'));
			}
		});
		const response = new page.mainFrame.window.Response(body);
		const rejected = expect(response.text()).rejects.toMatchObject({ name: 'AbortError' });
		await setImmediate();
		await page.close();
		await rejected;
		await browser.close();
	});

	it('Reads a fully downloaded network body after close.', async () => {
		const browser = new Browser();
		const window = browser.newPage().mainFrame.window;
		const nodeStream = new PassThrough();
		const response = new window.Response(FetchBodyUtility.nodeToWebStream(nodeStream));
		nodeStream.end(Buffer.from('downloaded'));
		await once(nodeStream, 'end');
		await browser.close();
		expect(await response.text()).toBe('downloaded');
	});

	it('Clears timeouts, intervals and animation frames on page close.', async () => {
		const browser = new Browser();
		const page = browser.newPage();
		const window = page.mainFrame.window;
		const callback = vi.fn();
		window.setTimeout(callback, 5);
		window.setInterval(callback, 5);
		window.requestAnimationFrame(callback);
		await page.close();
		// Attempts by scripts still running during teardown must not schedule new callbacks.
		window.setTimeout(callback, 0);
		window.setInterval(callback, 0);
		window.requestAnimationFrame(callback);
		await setTimeout(20);
		expect(callback).not.toHaveBeenCalled();
		await browser.close();
	});
});
