import type { ErrorResponse } from './types';

const JSON_HEADERS = { 'content-type': 'application/json; charset=utf-8' };

export function jsonOk<T extends object>(data: T): Response {
  return new Response(JSON.stringify({ ok: true, ...data }), {
    status: 200,
    headers: JSON_HEADERS,
  });
}

export function jsonError(code: number, error: string): Response {
  const body: ErrorResponse = { ok: false, code, error };
  return new Response(JSON.stringify(body), {
    status: code,
    headers: JSON_HEADERS,
  });
}
