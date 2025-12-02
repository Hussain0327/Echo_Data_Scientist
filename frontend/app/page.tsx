'use client';

import { useState } from 'react';
import Link from 'next/link';
import Navigation from '@/components/Navigation';
import FileUpload from '@/components/FileUpload';
import MetricsCard from '@/components/MetricsCard';
import { api, ApiError } from '@/lib/api';

export default function Home() {
  const [file, setFile] = useState<File | null>(null);
  const [metrics, setMetrics] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFileSelect = async (selectedFile: File) => {
    setFile(selectedFile);
    setError(null);
    setLoading(true);

    try {
      // Don't specify metrics - let backend auto-detect and calculate all applicable ones
      const result = await api.uploadAndCalculateMetrics(selectedFile);
      setMetrics(result);
    } catch (err) {
      console.error('Full error:', err);
      if (err instanceof ApiError) {
        setError(`API Error (${err.status}): ${err.message}`);
      } else if (err instanceof TypeError) {
        setError(`Network Error: ${err.message}. Make sure backend is running on http://localhost:8000`);
      } else {
        setError(`Error: ${err instanceof Error ? err.message : 'Unknown error'}`);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#0f172a]">
      <Navigation />

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {/* Hero Section */}
        <div className="text-center mb-12 animate-fade-in">
          <h1 className="text-4xl md:text-5xl font-bold mb-4">
            <span className="gradient-text">Analyze Your Business Data</span>
          </h1>
          <p className="text-lg text-slate-400 max-w-2xl mx-auto">
            Upload your CSV or Excel file to get instant metrics and AI-powered insights.
            Turn hours of manual analysis into minutes.
          </p>
        </div>

        {/* Upload Section */}
        <div className="mb-12 max-w-2xl mx-auto">
          <FileUpload onFileSelect={handleFileSelect} />
        </div>

        {/* Loading State */}
        {loading && (
          <div className="text-center py-12 animate-fade-in">
            <div className="inline-block relative">
              <div className="w-16 h-16 rounded-full border-4 border-slate-700 border-t-blue-500 animate-spin"></div>
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="w-8 h-8 rounded-full bg-blue-500/20 animate-pulse"></div>
              </div>
            </div>
            <p className="mt-4 text-slate-400">Analyzing your data...</p>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="max-w-2xl mx-auto mb-8 animate-fade-in">
            <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-6">
              <div className="flex items-start space-x-3">
                <div className="flex-shrink-0">
                  <svg className="w-6 h-6 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <div>
                  <p className="text-red-400 font-medium">{error}</p>
                  <p className="text-sm text-red-400/70 mt-2">
                    Make sure your backend is running: <code className="bg-red-500/20 px-2 py-0.5 rounded text-red-300">docker-compose up -d</code>
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Metrics Display */}
        {metrics && !loading && (
          <div className="animate-fade-in">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-2xl font-bold text-white flex items-center">
                <span className="w-2 h-8 bg-gradient-to-b from-blue-500 to-purple-600 rounded-full mr-3"></span>
                Your Metrics
              </h3>
              {metrics.data_type && (
                <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                  metrics.data_type === 'revenue' ? 'bg-green-500/20 text-green-400' :
                  metrics.data_type === 'marketing' ? 'bg-purple-500/20 text-purple-400' :
                  'bg-slate-500/20 text-slate-400'
                }`}>
                  {metrics.data_type === 'revenue' ? '💰 Revenue Data' :
                   metrics.data_type === 'marketing' ? '📊 Marketing Data' :
                   '📋 General Data'}
                </span>
              )}
            </div>
            {/* Show metrics if we have any */}
            {metrics.results && metrics.results.length > 0 ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                {metrics.results.map((metric: any, index: number) => (
                  <div key={metric.metric_name} style={{ animationDelay: `${index * 100}ms` }} className="animate-fade-in">
                    <MetricsCard
                      title={metric.metric_name.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())}
                      value={typeof metric.value === 'number' ?
                        (metric.unit === '%' ? metric.value.toFixed(2) : metric.value.toLocaleString())
                        : metric.value}
                      unit={metric.unit}
                      description={
                        metric.metadata?.total_leads ? `${metric.metadata.total_leads.toLocaleString()} leads` :
                        metric.metadata?.transaction_count ? `${metric.metadata.transaction_count} transactions` :
                        metric.metadata?.channel_count ? `${metric.metadata.channel_count} channels` :
                        undefined
                      }
                    />
                  </div>
                ))}
              </div>
            ) : (
              <div className="bg-amber-500/10 border border-amber-500/20 rounded-xl p-6 mb-8">
                <div className="flex items-start space-x-3">
                  <svg className="w-6 h-6 text-amber-400 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                  <div>
                    <p className="text-amber-400 font-medium">No metrics could be calculated</p>
                    <p className="text-sm text-amber-400/70 mt-1">
                      {metrics.message || `Your data has columns: ${metrics.columns?.join(', ')}. For revenue metrics, include an 'amount' column. For marketing metrics, include 'leads' and 'conversions' columns.`}
                    </p>
                  </div>
                </div>
              </div>
            )}

            {/* Next Steps */}
            <div className="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
              <h4 className="font-semibold text-white mb-4 flex items-center">
                <svg className="w-5 h-5 text-blue-400 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                </svg>
                Next Steps
              </h4>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Link href="/chat" className="group flex items-center p-4 bg-slate-700/30 rounded-lg border border-slate-600/50 hover:border-blue-500/50 hover:bg-slate-700/50 transition-all">
                  <div className="w-10 h-10 rounded-lg bg-blue-500/20 flex items-center justify-center mr-4 group-hover:bg-blue-500/30 transition-colors">
                    <svg className="w-5 h-5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                    </svg>
                  </div>
                  <div>
                    <p className="text-white font-medium group-hover:text-blue-400 transition-colors">Chat with Echo</p>
                    <p className="text-sm text-slate-400">Ask questions about your data</p>
                  </div>
                </Link>
                <Link href="/reports" className="group flex items-center p-4 bg-slate-700/30 rounded-lg border border-slate-600/50 hover:border-purple-500/50 hover:bg-slate-700/50 transition-all">
                  <div className="w-10 h-10 rounded-lg bg-purple-500/20 flex items-center justify-center mr-4 group-hover:bg-purple-500/30 transition-colors">
                    <svg className="w-5 h-5 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                  </div>
                  <div>
                    <p className="text-white font-medium group-hover:text-purple-400 transition-colors">Generate a Report</p>
                    <p className="text-sm text-slate-400">Get detailed analysis with AI insights</p>
                  </div>
                </Link>
              </div>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!file && !loading && !error && (
          <div className="text-center py-12 animate-fade-in">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-slate-800 mb-4">
              <svg className="w-8 h-8 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
              </svg>
            </div>
            <p className="text-slate-400 mb-4">Upload a file to get started</p>
            <div className="inline-flex flex-col items-start text-left bg-slate-800/50 rounded-lg p-4 border border-slate-700/50">
              <p className="text-xs text-slate-500 mb-2">Sample data available:</p>
              <code className="text-xs text-blue-400 font-mono">data/samples/revenue_sample.csv</code>
              <code className="text-xs text-blue-400 font-mono">data/samples/marketing_sample.csv</code>
            </div>
          </div>
        )}

        {/* Features Section */}
        {!metrics && !loading && (
          <div className="mt-16 grid grid-cols-1 md:grid-cols-3 gap-6 animate-fade-in">
            <div className="bg-slate-800/30 rounded-xl p-6 border border-slate-700/50 card-hover">
              <div className="w-12 h-12 rounded-lg bg-green-500/20 flex items-center justify-center mb-4">
                <svg className="w-6 h-6 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
              <h3 className="text-lg font-semibold text-white mb-2">Accurate Metrics</h3>
              <p className="text-slate-400 text-sm">Deterministic calculations you can trust. No AI hallucinations on the math.</p>
            </div>
            <div className="bg-slate-800/30 rounded-xl p-6 border border-slate-700/50 card-hover">
              <div className="w-12 h-12 rounded-lg bg-blue-500/20 flex items-center justify-center mb-4">
                <svg className="w-6 h-6 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <h3 className="text-lg font-semibold text-white mb-2">Lightning Fast</h3>
              <p className="text-slate-400 text-sm">Turn 2-hour manual reports into 15-minute automated insights.</p>
            </div>
            <div className="bg-slate-800/30 rounded-xl p-6 border border-slate-700/50 card-hover">
              <div className="w-12 h-12 rounded-lg bg-purple-500/20 flex items-center justify-center mb-4">
                <svg className="w-6 h-6 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
              </div>
              <h3 className="text-lg font-semibold text-white mb-2">AI Insights</h3>
              <p className="text-slate-400 text-sm">Natural language explanations powered by AI. Ask follow-up questions.</p>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
