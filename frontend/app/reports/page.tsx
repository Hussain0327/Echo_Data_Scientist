'use client';

import { useState } from 'react';
import Navigation from '@/components/Navigation';
import FileUpload from '@/components/FileUpload';
import { api } from '@/lib/api';

const reportTemplates = [
  {
    id: 'revenue_health',
    name: 'Revenue Health',
    description: 'Analyze revenue trends, growth rates, and identify opportunities',
    icon: (
      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
    ),
    color: 'green',
  },
  {
    id: 'marketing_funnel',
    name: 'Marketing Funnel',
    description: 'Track conversion rates, channel performance, and lead velocity',
    icon: (
      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
      </svg>
    ),
    color: 'blue',
  },
  {
    id: 'financial_overview',
    name: 'Financial Overview',
    description: 'CAC, LTV, burn rate, runway, and unit economics',
    icon: (
      <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
      </svg>
    ),
    color: 'purple',
  },
];

const colorClasses = {
  green: {
    bg: 'bg-green-500/20',
    text: 'text-green-400',
    border: 'border-green-500/50',
    ring: 'ring-green-500/50',
  },
  blue: {
    bg: 'bg-blue-500/20',
    text: 'text-blue-400',
    border: 'border-blue-500/50',
    ring: 'ring-blue-500/50',
  },
  purple: {
    bg: 'bg-purple-500/20',
    text: 'text-purple-400',
    border: 'border-purple-500/50',
    ring: 'ring-purple-500/50',
  },
};

export default function ReportsPage() {
  const [file, setFile] = useState<File | null>(null);
  const [templateType, setTemplateType] = useState('revenue_health');
  const [report, setReport] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleGenerateReport = async () => {
    if (!file) return;

    setLoading(true);
    setError(null);

    try {
      const result = await api.generateReport(file, templateType);
      setReport(result);
    } catch (err: any) {
      setError(err.message || 'Failed to generate report');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const selectedTemplate = reportTemplates.find(t => t.id === templateType);
  const colors = colorClasses[selectedTemplate?.color as keyof typeof colorClasses] || colorClasses.blue;

  return (
    <div className="min-h-screen bg-[#0f172a]">
      <Navigation />

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8 animate-fade-in">
          <h1 className="text-3xl font-bold text-white mb-2">
            Generate <span className="gradient-text">Report</span>
          </h1>
          <p className="text-slate-400">
            Create structured business reports with AI-generated insights
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Left Panel - Configuration */}
          <div className="lg:col-span-1 space-y-6">
            {/* Template Selection */}
            <div className="bg-slate-800/50 rounded-xl p-5 border border-slate-700/50 animate-fade-in">
              <h3 className="text-white font-medium mb-4 flex items-center">
                <svg className="w-5 h-5 text-blue-400 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z" />
                </svg>
                Report Type
              </h3>
              <div className="space-y-3">
                {reportTemplates.map((template) => {
                  const tColors = colorClasses[template.color as keyof typeof colorClasses];
                  const isSelected = templateType === template.id;
                  return (
                    <button
                      key={template.id}
                      onClick={() => setTemplateType(template.id)}
                      className={`w-full text-left p-4 rounded-xl border transition-all ${
                        isSelected
                          ? `${tColors.bg} ${tColors.border} ring-2 ${tColors.ring}`
                          : 'bg-slate-700/30 border-slate-600/50 hover:border-slate-500/50'
                      }`}
                    >
                      <div className="flex items-start space-x-3">
                        <div className={`p-2 rounded-lg ${tColors.bg} ${tColors.text}`}>
                          {template.icon}
                        </div>
                        <div>
                          <p className={`font-medium ${isSelected ? tColors.text : 'text-white'}`}>
                            {template.name}
                          </p>
                          <p className="text-xs text-slate-400 mt-1">{template.description}</p>
                        </div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* File Upload */}
            <div className="bg-slate-800/50 rounded-xl p-5 border border-slate-700/50 animate-fade-in" style={{ animationDelay: '50ms' }}>
              <h3 className="text-white font-medium mb-4 flex items-center">
                <svg className="w-5 h-5 text-blue-400 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                </svg>
                Upload Data
              </h3>
              <FileUpload onFileSelect={setFile} />
              {file && (
                <div className="mt-3 flex items-center space-x-2 p-2 bg-green-500/10 rounded-lg border border-green-500/20">
                  <svg className="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span className="text-sm text-green-400 truncate">{file.name}</span>
                </div>
              )}
            </div>

            {/* Generate Button */}
            <button
              onClick={handleGenerateReport}
              disabled={!file || loading}
              className="w-full py-4 bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded-xl font-medium hover:from-blue-500 hover:to-purple-500 disabled:from-slate-600 disabled:to-slate-600 disabled:cursor-not-allowed transition-all shadow-lg shadow-blue-500/25 disabled:shadow-none animate-fade-in"
              style={{ animationDelay: '100ms' }}
            >
              {loading ? (
                <span className="flex items-center justify-center">
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Generating Report...
                </span>
              ) : (
                <span className="flex items-center justify-center">
                  <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                  Generate Report
                </span>
              )}
            </button>

            {/* Error Message */}
            {error && (
              <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-4 animate-fade-in">
                <div className="flex items-start space-x-3">
                  <svg className="w-5 h-5 text-red-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <p className="text-sm text-red-400">{error}</p>
                </div>
              </div>
            )}
          </div>

          {/* Right Panel - Report Display */}
          <div className="lg:col-span-2">
            {loading && (
              <div className="bg-slate-800/50 rounded-xl border border-slate-700/50 p-12 text-center animate-fade-in">
                <div className="inline-block relative mb-4">
                  <div className="w-16 h-16 rounded-full border-4 border-slate-700 border-t-blue-500 animate-spin"></div>
                  <div className="absolute inset-0 flex items-center justify-center">
                    <div className="w-8 h-8 rounded-full bg-blue-500/20 animate-pulse"></div>
                  </div>
                </div>
                <p className="text-slate-400">Generating your report...</p>
                <p className="text-xs text-slate-500 mt-2">This may take a moment</p>
              </div>
            )}

            {report && !loading && (
              <div className="bg-slate-800/50 rounded-xl border border-slate-700/50 overflow-hidden animate-fade-in">
                {/* Report Header */}
                <div className="px-8 py-6 border-b border-slate-700/50 bg-slate-800/30">
                  <div className="flex items-center justify-between">
                    <div>
                      <h2 className="text-2xl font-bold text-white">
                        {report.report_type?.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())}
                      </h2>
                      <p className="text-sm text-slate-400 mt-1">
                        Generated {new Date(report.generated_at).toLocaleString()}
                      </p>
                    </div>
                    <div className={`p-3 rounded-xl ${colors.bg}`}>
                      {selectedTemplate?.icon && (
                        <div className={colors.text}>{selectedTemplate.icon}</div>
                      )}
                    </div>
                  </div>
                </div>

                {/* Report Content */}
                <div className="p-8 space-y-8">
                  {/* Executive Summary */}
                  {report.narratives?.executive_summary && (
                    <section>
                      <h3 className="text-lg font-semibold text-white mb-3 flex items-center">
                        <span className="w-1.5 h-6 bg-blue-500 rounded-full mr-3"></span>
                        Executive Summary
                      </h3>
                      <p className="text-slate-300 leading-relaxed bg-slate-700/30 rounded-xl p-4 border border-slate-600/50">
                        {report.narratives.executive_summary}
                      </p>
                    </section>
                  )}

                  {/* Key Findings */}
                  {report.narratives?.key_findings && report.narratives.key_findings.length > 0 && (
                    <section>
                      <h3 className="text-lg font-semibold text-white mb-3 flex items-center">
                        <span className="w-1.5 h-6 bg-green-500 rounded-full mr-3"></span>
                        Key Findings
                      </h3>
                      <ul className="space-y-2">
                        {report.narratives.key_findings.map((finding: string, idx: number) => (
                          <li key={idx} className="flex items-start space-x-3 text-slate-300">
                            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-green-500/20 text-green-400 flex items-center justify-center text-xs font-medium">
                              {idx + 1}
                            </span>
                            <span>{finding}</span>
                          </li>
                        ))}
                      </ul>
                    </section>
                  )}

                  {/* Detailed Analysis */}
                  {report.narratives?.detailed_analysis && (
                    <section>
                      <h3 className="text-lg font-semibold text-white mb-3 flex items-center">
                        <span className="w-1.5 h-6 bg-purple-500 rounded-full mr-3"></span>
                        Detailed Analysis
                      </h3>
                      <p className="text-slate-300 leading-relaxed whitespace-pre-wrap">
                        {report.narratives.detailed_analysis}
                      </p>
                    </section>
                  )}

                  {/* Recommendations */}
                  {report.narratives?.recommendations && report.narratives.recommendations.length > 0 && (
                    <section>
                      <h3 className="text-lg font-semibold text-white mb-3 flex items-center">
                        <span className="w-1.5 h-6 bg-yellow-500 rounded-full mr-3"></span>
                        Recommendations
                      </h3>
                      <ul className="space-y-2">
                        {report.narratives.recommendations.map((rec: string, idx: number) => (
                          <li key={idx} className="flex items-start space-x-3 text-slate-300">
                            <svg className="w-5 h-5 text-yellow-400 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                            </svg>
                            <span>{rec}</span>
                          </li>
                        ))}
                      </ul>
                    </section>
                  )}

                  {/* Metrics Grid */}
                  {report.metrics && Object.keys(report.metrics).length > 0 && (
                    <section>
                      <h3 className="text-lg font-semibold text-white mb-3 flex items-center">
                        <span className="w-1.5 h-6 bg-blue-500 rounded-full mr-3"></span>
                        Metrics
                      </h3>
                      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                        {Object.entries(report.metrics).map(([key, value]: [string, any]) => (
                          <div key={key} className="bg-slate-700/30 rounded-xl p-4 border border-slate-600/50">
                            <p className="text-xs text-slate-400 uppercase tracking-wide mb-1">
                              {key.replace(/([A-Z])/g, ' $1').trim()}
                            </p>
                            <p className="text-xl font-semibold text-white">
                              {typeof value.value === 'number' ? value.value.toLocaleString() : value.value}
                              {value.unit && <span className="text-sm text-slate-400 ml-1">{value.unit}</span>}
                            </p>
                          </div>
                        ))}
                      </div>
                    </section>
                  )}
                </div>
              </div>
            )}

            {!report && !loading && (
              <div className="bg-slate-800/50 rounded-xl border border-slate-700/50 p-12 text-center animate-fade-in">
                <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-slate-700/50 mb-4">
                  <svg className="w-8 h-8 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                </div>
                <p className="text-slate-400 mb-2">Your report will appear here</p>
                <p className="text-xs text-slate-500">Select a report type and upload data to get started</p>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}
