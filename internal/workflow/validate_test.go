package workflow

import "testing"

func TestValidateOrdersDependenciesDeterministically(t *testing.T) {
	def := Definition{ID:"build", Name:"Build", Steps:[]Step{
		{ID:"publish",Name:"Publish",Kind:"task",DependsOn:[]string{"test","package"}},
		{ID:"test",Name:"Test",Kind:"task",DependsOn:[]string{"install"}},
		{ID:"package",Name:"Package",Kind:"task",DependsOn:[]string{"install"}},
		{ID:"install",Name:"Install",Kind:"task"},
	}}
	order, err := Validate(def); if err != nil { t.Fatal(err) }
	position := map[string]int{}; for i,s := range order { position[s.ID]=i }
	for _,s := range def.Steps { for _,dep := range s.DependsOn { if position[dep]>=position[s.ID] { t.Fatalf("%s scheduled before %s",s.ID,dep) } } }
	order2, err := Validate(def); if err != nil { t.Fatal(err) }
	for i := range order { if order[i].ID != order2[i].ID { t.Fatal("order is not deterministic") } }
}
func TestValidateRejectsInvalidGraphs(t *testing.T) {
	cases := []struct{name string; steps []Step}{
		{"empty",nil},
		{"unknown dependency",[]Step{{ID:"a",Name:"A",Kind:"task",DependsOn:[]string{"missing"}}}},
		{"cycle",[]Step{{ID:"a",Name:"A",Kind:"task",DependsOn:[]string{"b"}},{ID:"b",Name:"B",Kind:"task",DependsOn:[]string{"a"}}}},
		{"duplicate",[]Step{{ID:"a",Name:"A",Kind:"task"},{ID:"a",Name:"Again",Kind:"task"}}},
		{"unsupported kind",[]Step{{ID:"a",Name:"A",Kind:"shell"}}},
	}
	for _,tc := range cases { t.Run(tc.name,func(t *testing.T){ _,err:=Validate(Definition{ID:"x",Name:"X",Steps:tc.steps}); if err==nil { t.Fatal("expected error") } }) }
}
