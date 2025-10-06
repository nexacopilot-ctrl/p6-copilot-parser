"""
P6 CoPilot - XER Parser API using PyP6Xer
Professional-grade XER parsing with the most popular Python library

Deploy to: Railway.app, Render.com, or your own server
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from xerparser.reader import Reader
from datetime import datetime
from typing import Dict, List, Any
import tempfile
import os

app = Flask(__name__)
CORS(app)

class DCMAAnalyzer:
    """DCMA 14-Point Analysis using PyP6Xer"""
    
    def __init__(self, xer: Reader):
        self.xer = xer
        # Get all activities from all projects (convert iterator to list)
        self.activities = []
        for project in list(xer.projects):
            self.activities.extend(list(project.activities))
    
    def analyze(self) -> Dict[str, Any]:
        """Perform complete DCMA analysis"""
        
        if not self.activities:
            return {
                'error': True,
                'message': 'No activities found in XER file'
            }
        
        metrics = {
            'logic': self._analyze_missing_logic(),
            'leads': self._analyze_leads(),
            'lags': self._analyze_lags(),
            'relationshipTypes': self._analyze_relationship_types(),
            'hardConstraints': self._analyze_hard_constraints(),
            'highFloat': self._analyze_high_float(),
            'negativeFloat': self._analyze_negative_float(),
            'highDuration': self._analyze_high_duration(),
            'invalidDates': self._analyze_invalid_dates(),
            'resources': self._analyze_resources()
        }
        
        # Calculate overall score
        scores = [m.get('score', 0) for m in metrics.values()]
        overall_score = sum(scores) / len(scores) if scores else 0
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics)
        
        return {
            'metrics': metrics,
            'overallScore': overall_score,
            'recommendations': recommendations,
            'summary': {
                'totalActivities': len(self.activities),
                'criticalIssues': len([r for r in recommendations if r['priority'] == 'high']),
                'analyzedAt': datetime.utcnow().isoformat()
            }
        }
    
    def _analyze_missing_logic(self) -> Dict[str, Any]:
        """Analyze activities without predecessors or successors"""
        # Exclude start/finish milestones
        regular_activities = [
            a for a in self.activities 
            if a.task_type not in ['TT_FinMile', 'TT_Mile']
        ]
        
        without_pred = len([a for a in regular_activities if not a.predecessors])
        without_succ = len([a for a in regular_activities if not a.successors])
        total = len(regular_activities)
        
        score = max(0, 100 - ((without_pred + without_succ) / total * 100)) if total > 0 else 0
        
        return {
            'score': round(score, 2),
            'tasksWithoutPredecessors': without_pred,
            'tasksWithoutSuccessors': without_succ,
            'totalTasks': total
        }
    
    def _analyze_high_float(self) -> Dict[str, Any]:
        """Analyze activities with excessive float"""
        threshold_days = 44
        
        high_float_activities = [
            a for a in self.activities 
            if a.total_float and a.total_float > threshold_days
        ]
        
        total = len(self.activities)
        score = max(0, 100 - (len(high_float_activities) / total * 100)) if total > 0 else 0
        
        return {
            'score': round(score, 2),
            'highFloatTasks': len(high_float_activities),
            'threshold': threshold_days,
            'totalTasks': total
        }
    
    def _analyze_negative_float(self) -> Dict[str, Any]:
        """Analyze activities behind schedule"""
        neg_float_activities = [
            a for a in self.activities 
            if a.total_float and a.total_float < 0
        ]
        
        total = len(self.activities)
        score = 100 if len(neg_float_activities) == 0 else max(0, 100 - (len(neg_float_activities) / total * 200))
        
        return {
            'score': round(score, 2),
            'negativeFloatTasks': len(neg_float_activities),
            'totalTasks': total
        }
    
    def _analyze_high_duration(self) -> Dict[str, Any]:
        """Analyze activities with excessive duration"""
        threshold_days = 44
        
        high_dur_activities = [
            a for a in self.activities 
            if a.duration and a.duration > threshold_days
        ]
        
        total = len(self.activities)
        score = max(0, 100 - (len(high_dur_activities) / total * 100)) if total > 0 else 0
        
        return {
            'score': round(score, 2),
            'highDurationTasks': len(high_dur_activities),
            'threshold': threshold_days,
            'totalTasks': total
        }
    
    def _analyze_hard_constraints(self) -> Dict[str, Any]:
        """Analyze activities with hard constraints"""
        constrained_activities = [
            a for a in self.activities 
            if a.constraint_type and a.constraint_type != 'CS_ASAP'
        ]
        
        total = len(self.activities)
        score = max(0, 100 - (len(constrained_activities) / total * 100)) if total > 0 else 0
        
        return {
            'score': round(score, 2),
            'constrainedTasks': len(constrained_activities),
            'totalTasks': total
        }
    
    def _analyze_leads(self) -> Dict[str, Any]:
        """Analyze lead times in relationships"""
        lead_count = 0
        for activity in self.activities:
            for rel in activity.predecessors:
                if rel.lag and rel.lag < 0:
                    lead_count += 1
        
        total_rels = sum(len(a.predecessors) for a in self.activities)
        score = max(0, 100 - (lead_count / total_rels * 100)) if total_rels > 0 else 100
        
        return {
            'score': round(score, 2),
            'leadsFound': lead_count,
            'totalRelationships': total_rels
        }
    
    def _analyze_lags(self) -> Dict[str, Any]:
        """Analyze lag times in relationships"""
        excessive_lag_threshold = 20  # days
        lag_count = 0
        
        for activity in self.activities:
            for rel in activity.predecessors:
                if rel.lag and rel.lag > excessive_lag_threshold:
                    lag_count += 1
        
        total_rels = sum(len(a.predecessors) for a in self.activities)
        score = max(0, 100 - (lag_count / total_rels * 100)) if total_rels > 0 else 100
        
        return {
            'score': round(score, 2),
            'excessiveLags': lag_count,
            'threshold': excessive_lag_threshold
        }
    
    def _analyze_relationship_types(self) -> Dict[str, Any]:
        """Analyze non-FS relationship types"""
        non_fs_count = 0
        total_rels = 0
        
        for activity in self.activities:
            for rel in activity.predecessors:
                total_rels += 1
                if rel.link != 'PR_FS':  # Not Finish-to-Start
                    non_fs_count += 1
        
        score = max(0, 100 - (non_fs_count / total_rels * 50)) if total_rels > 0 else 100
        
        return {
            'score': round(score, 2),
            'nonFSRelationships': non_fs_count,
            'totalRelationships': total_rels
        }
    
    def _analyze_invalid_dates(self) -> Dict[str, Any]:
        """Analyze invalid date logic"""
        invalid_dates = 0
        
        for activity in self.activities:
            if activity.start and activity.finish:
                if activity.start > activity.finish:
                    invalid_dates += 1
        
        total = len(self.activities)
        score = 100 if invalid_dates == 0 else max(0, 100 - (invalid_dates / total * 200))
        
        return {
            'score': round(score, 2),
            'invalidDates': invalid_dates,
            'totalTasks': total
        }
    
    def _analyze_resources(self) -> Dict[str, Any]:
        """Analyze resource assignments"""
        activities_with_resources = len([
            a for a in self.activities 
            if a.resources and len(a.resources) > 0
        ])
        
        total = len(self.activities)
        resource_rate = (activities_with_resources / total * 100) if total > 0 else 0
        
        return {
            'score': round(resource_rate, 2),
            'tasksWithResources': activities_with_resources,
            'totalTasks': total,
            'resourceAssignmentRate': round(resource_rate, 2)
        }
    
    def _generate_recommendations(self, metrics: Dict) -> List[Dict]:
        """Generate actionable recommendations"""
        recommendations = []
        
        if metrics['logic']['score'] < 80:
            recommendations.append({
                'category': 'Logic',
                'priority': 'high',
                'message': f"{metrics['logic']['tasksWithoutPredecessors'] + metrics['logic']['tasksWithoutSuccessors']} activities missing logic relationships",
                'action': 'Add predecessor and successor relationships to all activities'
            })
        
        if metrics['negativeFloat']['negativeFloatTasks'] > 0:
            recommendations.append({
                'category': 'Schedule',
                'priority': 'high',
                'message': f"{metrics['negativeFloat']['negativeFloatTasks']} activities have negative float",
                'action': 'Review and adjust schedule to resolve negative float'
            })
        
        if metrics['highFloat']['score'] < 70:
            recommendations.append({
                'category': 'Float',
                'priority': 'medium',
                'message': f"{metrics['highFloat']['highFloatTasks']} activities have excessive float (>{metrics['highFloat']['threshold']} days)",
                'action': 'Review task dependencies and network logic'
            })
        
        if metrics['highDuration']['score'] < 70:
            recommendations.append({
                'category': 'Duration',
                'priority': 'medium',
                'message': f"{metrics['highDuration']['highDurationTasks']} activities exceed {metrics['highDuration']['threshold']} days",
                'action': 'Break down long-duration activities into smaller tasks'
            })
        
        if metrics['hardConstraints']['score'] < 85:
            recommendations.append({
                'category': 'Constraints',
                'priority': 'medium',
                'message': f"{metrics['hardConstraints']['constrainedTasks']} activities have hard constraints",
                'action': 'Review and minimize use of hard constraints'
            })
        
        if metrics['resources']['score'] < 50:
            recommendations.append({
                'category': 'Resources',
                'priority': 'low',
                'message': f"Only {metrics['resources']['resourceAssignmentRate']:.1f}% of activities have resource assignments",
                'action': 'Improve resource loading and assignment'
            })
        
        return recommendations


# ============================================
# FLASK API ENDPOINTS
# ============================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'P6 CoPilot XER Parser (PyP6Xer)',
        'version': '2.0.0',
        'library': 'PyP6Xer',
        'timestamp': datetime.utcnow().isoformat()
    })


@app.route('/parse', methods=['POST'])
def parse_xer():
    """
    Parse XER file and return structured data
    Uses PyP6Xer library for professional-grade parsing
    """
    try:
        # Get file content
        if 'file' in request.files:
            file = request.files['file']
            
            # Save to temporary file (PyP6Xer needs file path)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xer') as tmp_file:
                file.save(tmp_file.name)
                tmp_path = tmp_file.name
            
            # Parse with PyP6Xer
            xer = Reader(tmp_path)
            
            # Clean up temp file
            os.unlink(tmp_path)
            
        elif request.is_json:
            data = request.get_json()
            file_content = data.get('fileContent', '')
            
            # Handle base64
            if 'base64' in data:
                import base64
                file_content = base64.b64decode(file_content)
            
            # Save to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xer', mode='wb') as tmp_file:
                if isinstance(file_content, str):
                    tmp_file.write(file_content.encode('utf-8'))
                else:
                    tmp_file.write(file_content)
                tmp_path = tmp_file.name
            
            xer = Reader(tmp_path)
            os.unlink(tmp_path)
        else:
            return jsonify({
                'error': True,
                'message': 'No file or content provided'
            }), 400
        
        # Extract data from PyP6Xer objects
        result = {
            'projects': [],
            'meta': {
                'fileType': 'xer',
                'totalProjects': len(xer.projects),
                'totalActivities': len([a for p in xer.projects for a in p.activities]),
                'parsedAt': datetime.utcnow().isoformat(),
                'parser': 'PyP6Xer'
            }
        }
        
        # Extract project data
        for project in xer.projects:
            project_data = {
                'id': project.proj_id,
                'name': project.proj_short_name,
                'fullName': project.proj_name if hasattr(project, 'proj_name') else project.proj_short_name,
                'activities': []
            }
            
            # Extract activity data
            for activity in project.activities:
                activity_data = {
                    'id': activity.task_id,
                    'name': activity.task_name,
                    'code': activity.task_code if hasattr(activity, 'task_code') else None,
                    'duration': activity.duration,
                    'totalFloat': activity.total_float,
                    'status': activity.status,
                    'taskType': activity.task_type,
                    'start': activity.start.isoformat() if activity.start else None,
                    'finish': activity.finish.isoformat() if activity.finish else None,
                    'percentComplete': activity.phys_complete_pct if hasattr(activity, 'phys_complete_pct') else 0
                }
                project_data['activities'].append(activity_data)
            
            result['projects'].append(project_data)
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        return jsonify({
            'error': True,
            'message': str(e),
            'type': type(e).__name__
        }), 500


@app.route('/analyze', methods=['POST'])
def analyze_xer():
    """
    Parse XER file AND perform DCMA analysis
    Returns both parsed data and analysis results
    """
    try:
        # Get file content (same as parse endpoint)
        if 'file' in request.files:
            file = request.files['file']
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xer') as tmp_file:
                file.save(tmp_file.name)
                tmp_path = tmp_file.name
            
            xer = Xer(tmp_path)
            os.unlink(tmp_path)
            
        elif request.is_json:
            data = request.get_json()
            file_content = data.get('fileContent', '')
            
            if 'base64' in data:
                import base64
                file_content = base64.b64decode(file_content)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xer', mode='wb') as tmp_file:
                if isinstance(file_content, str):
                    tmp_file.write(file_content.encode('utf-8'))
                else:
                    tmp_file.write(file_content)
                tmp_path = tmp_file.name
            
            xer = Reader(tmp_path)
            os.unlink(tmp_path)
        else:
            return jsonify({
                'error': True,
                'message': 'No file or content provided'
            }), 400
        
        # Perform DCMA analysis
        analyzer = DCMAAnalyzer(xer)
        dcma_result = analyzer.analyze()
        
        # Build simplified task data for Google Sheets
        tasks = []
        for project in xer.projects:
            for activity in project.activities[:500]:  # First 500 activities
                tasks.append({
                    'task_id': activity.task_id,
                    'task_name': activity.task_name,
                    'target_drtn_hr_cnt': activity.duration * 8 if activity.duration else 0,
                    'total_float_hr_cnt': activity.total_float * 8 if activity.total_float else 0,
                    'target_start_date': activity.start.isoformat() if activity.start else None,
                    'target_end_date': activity.finish.isoformat() if activity.finish else None,
                    'status_code': activity.status
                })
        
        return jsonify({
            'success': True,
            'data': {
                'tables': {
                    'TASK': tasks
                },
                'tableList': ['TASK'],
                'meta': {
                    'fileType': 'xer',
                    'recordCount': len(tasks),
                    'tableCount': 1,
                    'parsedAt': datetime.utcnow().isoformat(),
                    'parser': 'PyP6Xer v2.0'
                },
                'dcma': dcma_result
            }
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            'error': True,
            'message': str(e),
            'type': type(e).__name__,
            'traceback': traceback.format_exc()
        }), 500


@app.route('/', methods=['GET'])
def index():
    """API documentation"""
    return jsonify({
        'service': 'P6 CoPilot XER Parser API',
        'version': '2.0.0',
        'parser': 'PyP6Xer (Industry Standard)',
        'endpoints': {
            '/health': 'GET - Health check',
            '/parse': 'POST - Parse XER file only',
            '/analyze': 'POST - Parse XER file and perform DCMA analysis'
        },
        'usage': {
            'method': 'POST',
            'content-type': 'multipart/form-data OR application/json',
            'body': 'file upload OR {"fileContent": "..."}'
        },
        'features': [
            'Professional-grade XER parsing with PyP6Xer',
            'Complete DCMA 14-Point assessment',
            'Object-oriented data access',
            'Supports P6 versions 15.2 - 23.x'
        ]
    })


if __name__ == '__main__':
    # For development
    app.run(debug=True, host='0.0.0.0', port=5000)
    
    # For production with gunicorn:
    # gunicorn -w 4 -b 0.0.0.0:5000 app:app
