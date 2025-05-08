import uuid
import jinja2
from IPython.display import HTML
from typing import Literal

FLOWCHART_TEMPLATE = """
flowchart LR
    {subgraph_mermaid}
    {edge_mermaid}
"""

MERMAID_HTML_TEMPLATE = """
<!doctype html>
<html>

<body>
<pre class="mermaid">
    %s
</pre>
</body>
<script type="module">
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
</script>
</html>
"""
class MermaidMixin():
    def to_mermaid_code(self
                        , type: Literal['flowchart', 'classdiagram']='flowchart'
                        , **kwargs)->str:
        """
        Covert the dependency diagram into a mermaid flowchart language;
        """
        if type.lower() == 'flowchart':
            return self._render_mermaid_flowchart(**kwargs)
        elif type.lower() == 'classdiagram':
            return self._render_mermaid_classdiagram(**kwargs)
        else:
            raise ValueError(f"Invalid type: {type}")
        
    def to_mermaid_raw_html(self, **kwargs)->str:
        """
        Render the dependency diagram into a raw html string;
        """
        raw_html = MERMAID_HTML_TEMPLATE%self.to_mermaid_code(**kwargs)
        return raw_html
    def to_mermaid(self, **kwargs)->HTML:
        """
        Render the dependency diagram into a html;
        """
        raw_html = self.to_mermaid_raw_html(**kwargs)
        html = HTML(raw_html)
        return html
    
    # specific rendering function
    def _render_mermaid_classdiagram(self,**kwargs)->str:
        # use the class diagram if DataFrame(s) are used as input
        pass
    
    def _render_mermaid_flowchart(self,**kwargs)->str:
        npar_func_ = '%s("%s")'# node_id, node_element_name
        npar_obj_ = '%s[/"%s"/]'# node_id, node_label
        # ninput_obj_ = '%s@{shape: circle, label: "%s"}'# node_id, node_label
        ninput_obj_ = '%s[/"%s"/]' # node_id, node_label
        edge_ = '%s --> %s'# source, target
        subgraph_ = 'subgraph %s [%s]\n\t%s\n\tend'# subgraph_id, label,node_parse

        mermaid_crd = {}
        mermaid_crd['subgraphs'] = {}
        mermaid_crd['edges'] = []

        g = self.igraph

        for v in g.vs:
            if v.indegree() == 0:
                node_line = ninput_obj_%(v['id'], v['label'])
            elif v['type'] == 'function':
                node_line = npar_func_%(v['id'], v['element'].__name__)
            elif v['type'] == 'object':
                node_line = npar_obj_%(v['id'], v['label'])
            stage_id = v['stage_id']
            if stage_id not in mermaid_crd['subgraphs'].keys():
                mermaid_crd['subgraphs'][stage_id] = []
            mermaid_crd['subgraphs'][stage_id] += [node_line]

        for edge in g.es:
            edge_line = edge_%(edge.source, edge.target)
            mermaid_crd['edges'] += [edge_line]
        
    
        subgraph_mermaid_list = []
        uuids = []
        for stage_id, node_lines_list in mermaid_crd['subgraphs'].items():
            node_lines = "\n\t".join(node_lines_list)
            if stage_id < 0: 
                label = "input"
            else:
                label = self._stages[int(stage_id)].name
            uid = uuid.uuid4()
            uuids += [uid]
            
            subgraph_mermaid_1 = subgraph_%(uid, label,node_lines)
            subgraph_mermaid_list += [subgraph_mermaid_1]

        subgraph_mermaid = "\n\t".join(subgraph_mermaid_list)
        edge_mermaid = "\n\t".join(mermaid_crd['edges'])

        mermaid_template = FLOWCHART_TEMPLATE
        mermaid_code = mermaid_template.format(subgraph_mermaid=subgraph_mermaid, edge_mermaid=edge_mermaid)
        return mermaid_code