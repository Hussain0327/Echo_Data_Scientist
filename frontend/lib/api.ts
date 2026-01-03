const API_BASE = '/api/proxy';

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let message = 'An error occurred';
    try {
      const data = await response.json();
      message = data.detail || data.message || JSON.stringify(data);
    } catch {
      message = await response.text();
    }
    throw new ApiError(message, response.status);
  }
  return response.json();
}

export const api = {
  /**
   * Upload a file and calculate metrics
   */
  async uploadAndCalculateMetrics(file: File, metrics?: string, category?: string) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    if (metrics) params.append('metrics', metrics);
    if (category) params.append('category', category);

    const queryString = params.toString() ? `?${params.toString()}` : '';

    const response = await fetch(`${API_BASE}/metrics/calculate/csv${queryString}`, {
      method: 'POST',
      body: formData,
    });

    return handleResponse(response);
  },

  /**
   * Send a chat message
   */
  async chat(message: string, sessionId?: string) {
    const response = await fetch(`${API_BASE}/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        message,
        session_id: sessionId,
      }),
    });

    return handleResponse(response);
  },

  /**
   * Chat with data - upload file and send message
   */
  async chatWithData(message: string, file: File, sessionId?: string) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    params.append('message', message);
    if (sessionId) params.append('session_id', sessionId);
    params.append('calculate_metrics', 'true');

    const response = await fetch(`${API_BASE}/chat/with-data?${params.toString()}`, {
      method: 'POST',
      body: formData,
    });

    return handleResponse(response);
  },

  /**
   * Load data into a chat session
   */
  async loadDataToSession(sessionId: string, file: File) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    params.append('session_id', sessionId);
    params.append('calculate_metrics', 'true');

    const response = await fetch(`${API_BASE}/chat/load-data?${params.toString()}`, {
      method: 'POST',
      body: formData,
    });

    return handleResponse(response);
  },

  /**
   * Get chat history for a session
   */
  async getChatHistory(sessionId: string) {
    const response = await fetch(`${API_BASE}/chat/history/${sessionId}`);
    return handleResponse(response);
  },

  /**
   * Clear a chat session
   */
  async clearSession(sessionId: string) {
    const response = await fetch(`${API_BASE}/chat/session/${sessionId}`, {
      method: 'DELETE',
    });
    return handleResponse(response);
  },

  /**
   * Generate a report
   */
  async generateReport(file: File, templateType: string, userId?: string) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    params.append('template_type', templateType);
    if (userId) params.append('user_id', userId);

    const response = await fetch(`${API_BASE}/reports/generate?${params.toString()}`, {
      method: 'POST',
      body: formData,
    });

    return handleResponse(response);
  },

  /**
   * Get available report templates
   */
  async getReportTemplates() {
    const response = await fetch(`${API_BASE}/reports/templates`);
    return handleResponse(response);
  },

  /**
   * Get a specific report
   */
  async getReport(reportId: string) {
    const response = await fetch(`${API_BASE}/reports/${reportId}`);
    return handleResponse(response);
  },

  /**
   * List user's reports
   */
  async listReports(userId?: string, limit?: number) {
    const params = new URLSearchParams();
    if (userId) params.append('user_id', userId);
    if (limit) params.append('limit', limit.toString());

    const queryString = params.toString() ? `?${params.toString()}` : '';
    const response = await fetch(`${API_BASE}/reports${queryString}`);
    return handleResponse(response);
  },

  /**
   * Get available metrics
   */
  async getAvailableMetrics() {
    const response = await fetch(`${API_BASE}/metrics/available`);
    return handleResponse(response);
  },

  /**
   * Analyze trend
   */
  async analyzeTrend(
    file: File,
    valueColumn: string,
    dateColumn?: string,
    period?: string
  ) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    params.append('value_column', valueColumn);
    if (dateColumn) params.append('date_column', dateColumn);
    if (period) params.append('period', period);

    const response = await fetch(`${API_BASE}/metrics/trend?${params.toString()}`, {
      method: 'POST',
      body: formData,
    });

    return handleResponse(response);
  },

  /**
   * Calculate growth
   */
  async calculateGrowth(
    file: File,
    valueColumn: string,
    dateColumn?: string,
    period?: string
  ) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    params.append('value_column', valueColumn);
    if (dateColumn) params.append('date_column', dateColumn);
    if (period) params.append('period', period);

    const response = await fetch(`${API_BASE}/metrics/growth?${params.toString()}`, {
      method: 'POST',
      body: formData,
    });

    return handleResponse(response);
  },

  /**
   * Health check
   */
  async healthCheck() {
    const response = await fetch(`${API_BASE}/health`);
    return handleResponse(response);
  },
};
